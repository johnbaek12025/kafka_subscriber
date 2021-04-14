import logging

logger = logging.getLogger(__name__)


# fixme: 이 부분을 ks_manager 로 빼주자
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
            mgr.disconnect()
