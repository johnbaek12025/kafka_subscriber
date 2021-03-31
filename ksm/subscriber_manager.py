import logging

logger = logging.getLogger(__name__)


class SubscriberManager(object):
    def __init__(self):
        self.use_db = False
        self.db_mgrs = {}

    def stop(self):
        self.disconnect_from_db()

    def disconnect_from_db(self):
        if not bool(self.db_mgrs):
            return

        for mgr_id in self.db_mgrs:
            mgr = self.db_mgrs[mgr_id]
            if mgr is None:
                continue
            print(f"disconnected from {mgr_id},{mgr}")
            mgr.disconnect()
