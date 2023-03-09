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

## To implement your own prioritization rules
- Implement a `self.build_xxxx_table(self, ...)` method under `ysp_bot.dataset.FANCDataset` (see [these examples](https://github.com/NeLy-EPFL/fanc-proofreading-priority-queue/blob/223a90dbd3e96bfa7eb3b6dc3f5948ee9351ffc1/ysp_bot/dataset.py#L255-L349)). This should be the majority of your code.
- Modify [`update_version`](https://github.com/NeLy-EPFL/fanc-proofreading-priority-queue/blob/223a90dbd3e96bfa7eb3b6dc3f5948ee9351ffc1/scripts/server.py#L55) so that your method from the previous step is run when there's a new dataset version (happens every hour). This is a one-liner.
- Under [`sample_one_segment`](https://github.com/NeLy-EPFL/fanc-proofreading-priority-queue/blob/223a90dbd3e96bfa7eb3b6dc3f5948ee9351ffc1/scripts/server.py#L91) in `script/server.py`, add a case in the if-elif-...-else block to define how a row from the table you generated should be transformed into a text message sent to the user. This is just string formatting; it should be no more than 3 lines of code.
- Don't worry about checking whether this table is still valid when the user queried it; this is handled already.

## RESTful API?
Might set it up at some point. See `ysp_bot/api_server` for skeleton. If this is done, `script/server.py` (the bot server) can actually be a lot cleaner.
