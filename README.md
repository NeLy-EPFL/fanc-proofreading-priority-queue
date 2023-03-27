# You Should Proofread This Neuron

Prioritizing segments to proofread in FANC.

> Thanks to Femke for telling me I shouldn't name it FancyPQ (FANC priority queue).

## Setting up the bot
- TODO: Add setup instruction that requires clicking on Wlack's website
- Create a virtual environment
- Install `FANC_auto_recon`
  - Clone [FANC_auto_recon](https://github.com/htem/FANC_auto_recon/tree/main/fanc) and install it
  - `cd` into it, then `pip install -e .` (`-e` because if `FANC_auto_recon` changes you can simply `git pull` to update)
  - For some reason pip installing from the GitHub link doesn't work, so install it manually.
- Install this package:
  - Clone it
  - `cd` into it, then `pip install -e .`
- Modify/create config and credential files
  - Change `ysp_bot/config.yaml` as needed
  - Create credential file at path noted in `ysp_bot/config.yaml`:
```YAML
braincircuits: xxxxxxxxxx
cave: xxxxxxxxxx

slack:
  bot_token: xoxb-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx
  app_token: xapp-x-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx
  signing_secret: xxxxxxxxxx
```
  - I ended up using `fanc` from `FANC_auto_recon` so a separate cave credential file is needed after all. Configure `~/.cloudvolume/secrets/cave-secret.json` following this format:
```JSON
{
    "token": "xxxxxxxxxx",
    "fanc_production_mar2021": "xxxxxxxxxx"
}
```
- Install `screen` so you can run the server in the background even after leaving the SSH session
  - `sudo apt update`
  - `sudo apt install screen`
- Running the server:
  - `screen` to enter a screen session
  - `python scripts/server.py`
  - `Ctrl+a`, then click `d` to detach from screen session
  - It's now safe to terminate the shell session. The server will run in the background.
  - To reconnect to the screen session, run `screen -r`

## Implement your own prioritization rules
The prioritization rules are implemented in `ysp_bot/rules.py`. Take a look at existing examples. Each rule should be implemented as a class that inherits from the `ysp_bot.rules.PrioritizationRule` abstract class. This abstract class requires you to implement two methods, namely `get_table` and `entry_to_feed`. They are specified below:
```Python
class PrioritizationRule(abc.ABC):
    @abc.abstractmethod
    def get_table(self, dataset: FANCDataset, *args, **kwargs) -> pd.DataFrame:
        """Given a `FANCDataset` object and a set of user-defined
        parameters, return a Pandas dataframe of segments that are
        selected for proofreading by this rule. This method is run
        once an hour when a new FANC data dump is downloaded.

        Parameters
        ----------
        dataset : FANCDataset
            The most up-to-date FANC dataset object. `See dataset.py`.
        *args, **kwargs
            You can define any additional parameters for the rule.

        Returns
        -------
        pd.DataFrame
            A Pandas dataframe with any columns you want, as long as
            they are indexed by the segment ID (integer). The columns
            will be used to generate the feed entry (message given to
            the proofreader).
        """
        pass    # implement your selection logic here
            
    @abc.abstractmethod
    def entry_to_feed(self, etr: pd.Series) -> Dict:
        """Given a single row in the table returned by `get_table`,
        convert it to a message to be given to the proofreader.

        Parameters
        ----------
        etr : pd.Series
            A row from the table. `etr.name` would be the segment ID,
            `etr['col_name']` would be the value in any column you
            defined in `get_table`. 

        Returns
        -------
        Dict
            A dictionary of this format:
            ```
            {
                'segid': etr.name,
                'type': 'your text here',  # eg. 'Orphaned soma'
                'reason': 'your text here'  # why you're proposing this segment
            }
            ```
        """
        pass    # convert a single row in the table to a feed entry
```

Concretely, to implement a new rule, you will just have to define something like this in `ysp_bot/rules.py`:
```Python
class MyRule(PrioritizationRule):
    def get_table(dataset, ...):
        table = ...
        return table
    
    def entry_to_feed(self, etr):
        return {
            'segid': etr.name,
            'type': 'My neuron type',
            'reason': (
                f'Some text briefly explaining the problem. You can use '
                f'columns in the table you defined ({etr["my_col"]}) to '
                f'format more info.'
            )
        }
```

Now, which attributes can we access from the `FANCDataset` object when implementing our new rule? For this, see `notebooks/fanc_dataset.ipynb` to get an overview of what's in `FANCDataset`.

Finally, you need to add your rule to the list of rules that the server uses. To do so, add an entry to the `rules_config` list at the very end of `ysp_bot/rules.py`. This entry should be a 3-tuple: the slack subcommand (str), the name of the table (str), and the class implementing the rule (class). You are free to define the subcommand and the table name but they shouldn't conflict with existing ones. For example, if you add the following entry to the list (assuming you have implemented the `MyRule` class above):
```Python
rules_config = [
    ...  # existing rules
    ('myrule', 'my_rule_table', MyRule)
]
```
Then every hour, a `my_rule_table` Pandas dataframe will be updated using the `MyRule` class, and every time the user messages the bot `/get myrule`, the bot will fetch a row from that table and give it to the user.

That's how you add a new rule! Don't worry about checking whether this table is still valid when the user queried it; this is handled already.

## RESTful API?
I haven't implemented this yet but might set it up at some point. See `ysp_bot/api_server` for skeleton. If this is done, `script/server.py` (the bot server) can actually be a lot cleaner.
