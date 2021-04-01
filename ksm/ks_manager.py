"""
- ctrl+c dose not work with DISABLE_OOB = ON in sqlnet.ora
- run the below code as a makeshift
  $ kill -9 `ps -ef | grep "python bin/ks-manager.py" | grep -v grep | awk '{print $2}'` 
"""


import logging
import time

from ksm.db_manager import DBManager
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
        # fixme: 아래 count 를 사용하기 위해서, kafka의 group과 partition 설정해야함
        self.subscribers = [
            {"sub_id": "analysis_content", "count": 1},
            {"sub_id": "rassiro_news", "count": 1},
        ]
        self.workers = {}

    @staticmethod
    def get_analysis_object(sub_id, **kafka_opts):
        try:
            exec(f"from .subs.{sub_id} import Subscriber")
        except ModuleNotFoundError as err:
            logger.info(f"Subscriber {sub_id}는 존재하지 않습니다: {err}")
            return None
        c = eval("Subscriber(**kafka_opts)")
        return c

    def initialize(self, config_dict, use_processes=True):
        kafka_opts = config_dict.get("kafka", {})
        for s in self.subscribers:
            sub_id = s["sub_id"]
            count = s["count"]
            proc = KSManager.get_analysis_object(sub_id, **kafka_opts)

            for i in range(count):
                sub_proc_id = f"{sub_id}_{i}"
                if proc.use_db:
                    try:
                        for db_id in proc.db_mgrs:
                            db_info = config_dict.get(db_id, {})
                            proc.db_mgrs[db_id] = KSManager.get_connected_mgr(
                                sub_proc_id, db_info
                            )
                    except AttributeError as err:
                        logger.info(f"{sub_proc_id}: {err}")
                        continue

                self.workers[sub_proc_id] = self.create_child(
                    sub_proc_id, proc, use_processes
                )

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
