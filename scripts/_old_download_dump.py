from datetime import datetime
from time import sleep

from fancpq._old_upstream_api.dump import get_new_dump


if __name__ == '__main__':
    """Check for new dump every hour at 15 minutes past (since
    BrainCircuits dumps are generated on the hour and it takes some
    time)."""
    
    print(f'* Checking for new dump...')
    get_new_dump()
    while True:
        # When should I check for a new dump?
        now = datetime.now()
        if now.minute < 15:
            check_time = datetime(now.year, now.month, now.day,
                                  now.hour, 15)
        else:
            check_time = datetime(now.year, now.month, now.day,
                                  now.hour + 1, 15)
        print(f'* Next check at {check_time}...')
        sleep((check_time - now).total_seconds())
        print(f'* Checking for new dump...')
        get_new_dump()