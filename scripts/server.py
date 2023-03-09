import logging
import threading
import gc
import re
import shutil
import random
from datetime import datetime, timedelta
from pathlib import Path
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from caveclient import CAVEclient

import ysp_bot
import ysp_bot.util


# Useful global variables
curr_version_timestamp = None
curr_version_dir = None
curr_pool = None
main_mutex = threading.Lock()

get_subcommands = {
    'soma': 'orphaned_soma_table',
    'somas': 'multiple_soma_table',
    'an': 'problematic_an_table',
    'mn': 'problematic_mn_table',
    'in': 'unbalanced_in_table'
}

config = ysp_bot.util.load_config()
credentials = ysp_bot.util.load_credentials()
cave_client = CAVEclient(datastack_name=config['cave']['dataset'],
                         auth_token=credentials['cave'])
data_dir = Path(config['local']['data']).expanduser()
data_dir.mkdir(parents=True, exist_ok=True)

log_path = data_dir / 'proofreading_server.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    handlers=[logging.FileHandler(log_path),
                              logging.StreamHandler()])

def seconds_till_next_run(minutes_past_hour):
    """Return the number of seconds till `minutes_past_hour` minutes
    past the next whole hour."""
    now = datetime.now()
    next_run = datetime(year=now.year, month=now.month, day=now.day,
                        hour=now.hour, minute=minutes_past_hour)
    if now > next_run:
        next_run += timedelta(hours=1)
    return max(0, (next_run - now).total_seconds())


def update_version(minutes_past_hour=5):
    global curr_version_timestamp, curr_version_dir, curr_pool, main_mutex
    
    logging.info('Checking for new version...')
    ds = ysp_bot.FANCDataset.get_latest()
    
    if ds.mat_timestamp == curr_version_timestamp:
        logging.warning(f'No new version found; '
                        f'still using version {curr_version_timestamp}')
    else:
        curr_version_timestamp = ds.mat_timestamp
        new_pool = {
            'orphaned_soma_table': ds.build_orphaned_soma_table(),
            'multiple_soma_table': ds.build_multiple_soma_table(),
            'problematic_an_table': ds.build_problematic_an_table(),
            'problematic_mn_table': ds.build_problematic_efferent_table(),
            'unbalanced_in_table': ds.build_unbalanced_interneuron_table()
        }
        logging.info('Calculated new problematic tables: ' +
                     str({k: len(v) for k, v in new_pool.items()}))
        main_mutex.acquire()
        curr_pool = new_pool
        logging.info('Datset version updated')
        main_mutex.release()
        if curr_version_dir is not None:
            logging.info(f'Removing old version at {curr_version_dir}')
            shutil.rmtree(curr_version_dir)
        curr_version_dir = ds.version_data_dir
        del ds
        gc.collect()  # Force garbage collection
    
    wait_time = seconds_till_next_run(minutes_past_hour)
    threading.Timer(wait_time, update_version,
                    kwargs={'minutes_past_hour': minutes_past_hour}).start()
    logging.info(f'Scheduled next version check in {wait_time} seconds')


def sample_one_segment(table, user):
    global curr_pool, main_mutex
    
    logging.info(f'Sampling one segment from {table} for {user}...')
    
    db = ysp_bot.ProofreadingDatabase(data_dir / 'proofreading.db')
    
    # If asked to sample from any table, randomly pick a table and
    # recursively call this function
    if table is None:
        sample_from = list(curr_pool.keys())
        random.shuffle(sample_from)
        for chosen_table in sample_from:
            result = sample_one_segment(chosen_table, user)
            if result is not None:
                return result
        return None
    
    # First exclude rows that are definitely not valid
    assert table in curr_pool.keys()
    main_mutex.acquire()
    user_skiplist = db.get_user_skiplist(user)
    global_invalid_list = db.get_global_segids_to_skip()
    segids_to_exclude = set.union(user_skiplist, global_invalid_list)
    valid_sel = curr_pool[table][
        ~curr_pool[table].index.isin(segids_to_exclude)
    ]
    main_mutex.release()
    
    # Check if this segid has been touched since the last dump
    # Iteratively find the first row that is still valid and return it
    valid_sel = valid_sel.sample(frac=1)    # shuffle rows first
    for segid, etr in valid_sel.iterrows():
        logging.debug(f'Checking {segid}...')
        if not cave_client.chunkedgraph.is_latest_roots([segid])[0]:
            logging.info(f'{segid} has been touched since last dump')
            db.set_status(segid, 'expired', 'SERVER')
            continue
        if table == 'orphaned_soma_table':
            type_ = 'Orphaned soma'
            message = (f'This soma is only attached to '
                        f'{int(etr["total_synapses"])} synapses.')
        elif table == 'multiple_soma_table':
            type_ = 'Multiple somas'
            message = (f'This neuron appears to have '
                        f'{etr["num_somas"]} somas.')
        elif table == 'problematic_an_table':
            type_ = 'Ascending neuron'
            message = (f'This ascending neuron only has '
                        f'{int(etr["nr_post"])} input synapses.')
        elif table == 'problematic_mn_table':
            type_ = 'Motor neuron'
            message = (f'This motor neuron only has '
                        f'{int(etr["nr_post"])} input synapses.')
        elif table == 'unbalanced_in_table':
            type_ = 'VNC interneuron'
            message = (f'This VNC interneuron has {int(etr["nr_post"])} '
                        f'input synapses and {int(etr["nr_pre"])} output '
                        f'synapses. This is quite unbalanced.')
        retval = {'segid': segid, 'type': type_, 'reason': message}
        logging.debug(f'Found valid segid from {table}: {retval}')
        db.close()
        return retval
    
    db.close()
    return None


def slack_find_segid_from_button_click(client, channel_id,
                                       interactive_message_ts):
    """Super sketchy. When the user clicks a button, slack calls the
    function decorated with @app.action('button-xxxx') with the
    argument `body`, which contains channel id and and the time at
    which the message (whose button the user clicked) was sent.
    Slack uses the timestamp as the unique identifier for a message!!
    
    This way, to find which segid the button is associated with, we
    find the two messages sent before (inclusive) the provided
    timestamp. The older message is the one that contains the buttons;
    the newer message is the one that contains the info on the neuron.
    We extract segid from there using regex.
    """
    try:
        res = client.conversations_history(channel=channel_id,
                                        latest=interactive_message_ts,
                                        inclusive=True, limit=2)
        messages = sorted(res.data['messages'], key=lambda x: float(x['ts']))
        message_with_neuron_info = messages[0]
        segid_info_text = message_with_neuron_info['blocks'][1]\
                                                  ['fields'][0]['text']
        re_finds = re.findall('\*Segment ID:\*\\n(\d+)', segid_info_text)
        segid = int(re_finds[0])
        return segid
    except (KeyError, IndexError, ValueError):
        logging.error('Could not find segid associated with the button '
                      'the user clicked.')
        return None


app = App(token=credentials['slack']['bot_token'],
          signing_secret=credentials['slack']['signing_secret'])


@app.event('app_home_opened')
def update_home_tab(client, event, logger):
    try:
        client.views_publish(
            user_id=event['user'],
            view={
                "type": "home",
                "callback_id": "home_view",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*WORK IN PROGRESS*"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Welcome to the FANC proofreading prioritization bot! You can interact with this bot in the \"Messages\" tab. It's assumed that you're aready trained and qualified to proofread the FANC dataset."
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "To get started, message the bot `/get` to get a segment to proofread."
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "You can also ask for more specific types of proofreading tasks, for example, `/get in` will give you a VNC interneuron to proofread. Currently supported selection keywords include:"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "• `in`: unbalanced VNC interneuron (segments with way fewer inputs or outputs than expected) \n • `soma`: orphaned somas that are disconnected from their arbor \n • `somas`: segments with more than one somas \n • `an`: ascending neuron (going from the VNC to the brain) with too few inputs \n • `mn`: motor neurons with too few inputs in the VNC"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "After a task is proposed to you, you will also see a few buttons. These are for you to (optionally) mark the neuron as \"fixed\" or \"skipped.\" Alternatively, you can also mark any other neuron as fully proofread by messaging me `/mark <segment-id> done`."
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Additionally, you can also add text annotations to neurons. To do so, message me `/annotate <segment-id> (<x-pos>, <y-pos>, <z-pos>) <annotation-content>`."
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"That's it! Feel free to message <@{config['slack']['admin']}> if you have any questions."
                        }
                    }
                ]
            }
        )
    except Exception as e:
        logger.error(f'Error publishing home tab: {e}')


@app.command('/get')
def propose_segment(client, ack, respond, command, say, body):
    ack()
    args = command['text'].split()
    
    if curr_pool is None:
        say(text='No pool is loaded yet. Please wait.')
        return
    say(text=(f':point_right: Your command was: `/get {command["text"]}`. '
              'I\'m working on it.'))
    
    table = get_subcommands[args[0]] if args else None
    feed = sample_one_segment(table=table, user=body['user_id'])
    if feed is None:
        say(text='No more segments to propose! :tada:')
        return
    
    say(
        text='@You Should Proofread this neuron!',
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "@You Should Proofread this neuron!"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Segment ID:*\n{feed['segid']}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Type:*\n{feed['type']}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Reason:*\n{feed['reason']}"
                    }
                ]
            }
        ]
    )
    say(text='[I fixed it] [Nothing wrong with this neuron] [Skip]',
        blocks=[
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "I fixed it"
                        },
                        "style": "primary",
                        "action_id": "button-fixed"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Nothing wrong with this neuron"
                        },
                        "style": "danger",
                        "action_id": "button-nothing-wrong"
                    },
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "Skip"
                        },
                        "action_id": "button-skip"
                    }
                ]
            }
        ]
    )


@app.action('button-fixed')
def respond_fixed_button(client, ack, body, say):
    ack()
    
    user = body['user']['username']
    channel_id = body['container']['channel_id']
    container_ts = body['container']['message_ts']
    
    logging.debug('Finding associated segid')
    segid = slack_find_segid_from_button_click(client, channel_id, container_ts)
    if segid is None:
        response = (':warning: Could not find the associated segment ID. '
                    'This is a bug. '
                    f"Please report it to <@{config['slack']['admin']}>.")
    else:
        logging.info(f'User {user} marked {segid} as fixed')
        logging.debug('Connecting to database')
        db = ysp_bot.ProofreadingDatabase(data_dir / 'proofreading.db')
        db.set_status(segid, 'fixed', user)
        db.close()
        response = ':tada: You marked this neuron as fixed!'
    
    client.chat_update(channel=body['container']['channel_id'],
                       ts=body['container']['message_ts'],
                       blocks=[{"type": "section",
                                "text": {"type": "mrkdwn",
                                         "text": response}}],
                       text=response)


@app.action('button-nothing-wrong')
def respond_noaction_button(client, ack, body, say):
    ack()
    
    user = body['user']['username']
    channel_id = body['container']['channel_id']
    container_ts = body['container']['message_ts']
    
    logging.debug('Finding associated segid')
    segid = slack_find_segid_from_button_click(client, channel_id, container_ts)
    if segid is None:
        response = (':warning: Could not find the associated segment ID. '
                    'This is a bug. '
                    f"Please report it to <@{config['slack']['admin']}>.")
    else:
        logging.info(f'User {user} marked {segid} as fixed')
        logging.debug('Connecting to database')
        db = ysp_bot.ProofreadingDatabase(data_dir / 'proofreading.db')
        db.set_status(segid, 'noaction', user)
        db.close()
        response = (':ok_hand: OK, no action taken, '
                    'this neuron is marked as done. Thanks for checking!')
    client.chat_update(channel=body['container']['channel_id'],
                       ts=body['container']['message_ts'],
                       blocks=[{"type": "section",
                                "text": {"type": "mrkdwn",
                                         "text": response}}],
                       text=response)


@app.action('button-skip')
def respond_skip_button(client, ack, body, say):
    ack()
    
    user = body['user']['username']
    channel_id = body['container']['channel_id']
    container_ts = body['container']['message_ts']
    
    logging.debug('Finding associated segid')
    segid = slack_find_segid_from_button_click(client, channel_id, container_ts)
    if segid is None:
        response = (':warning: Could not find the associated segment ID. '
                    'This is a bug. '
                    f"Please report it to <@{config['slack']['admin']}>.")
    else:
        logging.info(f'User {user} marked {segid} as fixed')
        logging.debug('Connecting to database')
        db = ysp_bot.ProofreadingDatabase(data_dir / 'proofreading.db')
        db.add_to_user_skiplist(user, segid)
        db.close()
        response = ':ok_hand: OK, I won\'t show this neuron to you again.'
    client.chat_update(channel=body['container']['channel_id'],
                       ts=body['container']['message_ts'],
                       blocks=[{"type": "section",
                                "text": {"type": "mrkdwn",
                                         "text": response}}],
                       text=response)


@app.command('/mark')
def mark_segment(ack, say, command):
    ack()
    try:
        match = re.match('(\d+)\s+(.+)', command['text'])
        segid = int(match.group(1))
        state = match[2]
    except:
        say(':exclamation: '
            f'Your command was: `/mark {command["text"]}`\n\n'
            'I can\'t process it becasue there aren\'t enough arguments. '
            'Please provide a segment ID and a status, for example '
            '`/mark 648518346504772979 done`')
        return
    user = command['user_name']
    logging.info(f'User {user} marked {segid} as {state}')
    
    logging.debug('Connecting to database')
    db = ysp_bot.ProofreadingDatabase(data_dir / 'proofreading.db')
    db.set_status(segid, state, user)
    db.close()
    
    say(f':point_right:'
        f'Your command was: `/mark {command["text"]}`\n\n'
        f'OK! I\'ve marked segment `{segid}` as {state}.')


@app.command('/annotate')
def annotate_segment(ack, say, command):
    ack()
    try:
        match = re.match('(\d+)\s+\((\d+),\s*(\d+),\s*(\d+)\)\s+(.+)',
                         command['text'])
        segid = int(match.group(1))
        pt_pos = (int(match.group(2)), int(match.group(3)), int(match.group(4)))
        message = match.group(5)
    except:
        say(':exclamation: '
            f'Your command was: `/annotate {command["text"]}`\n\n'
            'I can\'t process it becasue there aren\'t enough arguments. '
            'Please provide a segment ID, a position, and a status, for '
            'example: `/annotate 648518346504772979 (61849, 152027, 2462) '
            'This is a really cool neuron!`')
        return
    user = command['user_name']
    logging.info(f'User {user} annotated {segid} at {pt_pos} with: {message}')
    
    logging.debug('Connecting to database')
    db = ysp_bot.ProofreadingDatabase(data_dir / 'proofreading.db')
    db.set_annotation(segid, message, user, pt_pos)
    
    say(f':point_right: '
            f'Your command was: `/annotate {command["text"]}`\n\n'
            f'OK! I\'ve added the following message to segment `{segid}`: '
            f'"{message}"')


@app.event("message")
def handle_message_events(body, logger):
    logger.debug(f"Got message: {body}")


if __name__ == '__main__':
    update_version()
    
    handler = SocketModeHandler(app, credentials['slack']['app_token'])
    handler.start()
