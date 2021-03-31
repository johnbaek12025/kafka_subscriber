import logging
import time

from ksm.db_manager_ks import DBManager
from ksm.process_manager import ProcessorProcess, ProcessorThread, finalize_processor

logger = logging.getLogger(__name__)


class KSException(Exception):
    def __init__(self, error_msg):
        super().__init__(error_msg)


class KSManager(object):
    class Terminate(Exception):
        """Exception for terminating the application"""

    @staticmethod
    def sigTERMHandler(signum, dummy_frame):
        raise KSManager.Terminate

    def __init__(self):
        self.kafka_manager = None
        self.subscribers = ["test1", "test2"]
        self.workers = {}

    @staticmethod
    def get_analysis_object(sub_id, **kafka_opts):
        try:
            exec(f"from .subs.{sub_id} import Subscriber")
        except ModuleNotFoundError as err:
            # FIXME: 나중에, logger.info 로 변경
            print(f"Subscriber {sub_id}는 존재하지 않습니다: {err}")
            return None
        c = eval("Subscriber(**kafka_opts)")
        return c

    def initialize(self, config_dict, use_processes=True):
        kafka_opts = config_dict.get("kafka", {})
        for sub_id in self.subscribers:
            proc = KSManager.get_analysis_object(sub_id, **kafka_opts)
            if proc.use_db:
                try:
                    for db_id in proc.db_mgrs:
                        db_info = config_dict.get(db_id, {})
                        proc.db_mgrs[db_id] = KSManager.get_connected_mgr(
                            sub_id, db_info
                        )
                except AttributeError as err:
                    logger.info(f"{sub_id}: {err}")
                    continue

            self.workers[sub_id] = self.create_child(sub_id, proc, use_processes)

    def create_child(self, label, proc, use_processes):
        if not proc:
            return
        if not use_processes:
            return ProcessorThread(proc, label)
        return ProcessorProcess(proc, label)

    def run(self):
        for p_id in self.workers:
            child = self.workers[p_id]
            if child:
                logger.info("start %s" % p_id)
                child.start()
            else:
                logger.info("cannot start %s" % p_id)

        while True:
            try:
                time.sleep(0.1)
            except (KeyboardInterrupt, KSManager.Terminate):
                print(f"KeyboardInterrupted")
                break
        self.stop()

    def stop(self):
        finalize_processor(self.workers)
        logger.info("shutdown kafka subscribe manager")

    @staticmethod
    def get_connected_mgr(label, db_info):
        db_mgr = None
        if db_info is None:
            return db_mgr
        db_mgr = DBManager()
        db_mgr.connect(**db_info)
        return db_mgr
