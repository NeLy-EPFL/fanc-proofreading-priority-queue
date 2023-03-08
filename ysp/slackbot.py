from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

import fancpq
import fancpq.util


credentials = fancpq.util.load_credentials()

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
                            "text": "• `in`: unbalanced VNC interneuron (segments with way fewer inputs or outputs than expected) \n • `soma`: orphaned somas that are disconnected from their arbor \n • `an`: ascending neuron (going from the VNC to the brain) with too few inputs \n • `mn`: motor neurons with too few inputs in the VNC"
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
                            "text": "Additionally, you can also add text annotations to neurons. To do so, message me `/annotate <segment-id> <annotation-content>`."
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "That's it! Feel free to message <@U022J881TQC> if you have any questions."
                        }
                    }
                ]
            }
        )
    except Exception as e:
        logger.error(f'Error publishing home tab: {e}')


@app.command('/get')
def propose_segment(client, ack, respond, command, say):
    ack()
    # print(command)
    args = command['text'].split()
    # TODO
    say(
        text='Here\'s a segment for you!',
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (":point_right: "
                            f'Your command was: `/get {command["text"]}`\n\n'
                            'OK! Here\'s your neuron:')
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": "*Segment ID:*\n{segment_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": "*Type:*\n{type}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": "*Reason:*\n{what's wrong}"
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
    # TODO
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
    # TODO
    response = (':ok_hand: OK, no action taken, this neuron is marked as done. '
                'Thanks for checking!')
    client.chat_update(channel=body['container']['channel_id'],
                       ts=body['container']['message_ts'],
                       blocks=[{"type": "section",
                                "text": {"type": "mrkdwn",
                                         "text": response}}],
                       text=response)


@app.action('button-skip')
def respond_skip_button(client, ack, body, say):
    ack()
    # TODO
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
    args = command['text'].split()
    if len(args) < 2:
        say(':exclamation: '
                f'Your command was: `/mark {command["text"]}`\n\n'
                'I can\'t process it becasue there aren\'t enough arguments. '
                'Please provide a segment ID and a status, for example '
                '`/mark 648518346504772979 done`')
        return
    segid = args[0]
    states = args[1:]
    # TODO
    say(f':point_right:'
            f'Your command was: `/mark {command["text"]}`\n\n'
            f'OK! I\'ve marked segment `{segid}` as {" and ".join(states)}.')


@app.command('/annotate')
def annotate_segment(ack, say, command):
    ack()
    args = command['text'].split()
    if len(args) < 2:
        say(':exclamation: '
                f'Your command was: `/annotate {command["text"]}`\n\n'
                'I can\'t process it becasue there aren\'t enough arguments. '
                'Please provide a segment ID and a status, for example '
                '`/annotate 648518346504772979 This is a really cool neuron!')
        return
    segid = args[0]
    message = ' '.join(args[1:])
    # TODO
    say(f':point_right: '
            f'Your command was: `/annotate {command["text"]}`\n\n'
            f'OK! I\'ve added the following message to segment `{segid}`: '
            f'"{message}"')


if __name__ == '__main__':
    handler = SocketModeHandler(app, credentials['slack']['app_token'])
    handler.start()