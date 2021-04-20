# Copyright 2021 ThinkPool, all rights reserved
"""
- Ctrl + C 로 종료해야 함 (현재 개발에서는 되지만 상용에서는 안됨)
- 오라클 sqlnet.ora 중,  DISABLE_OOB 가 'ON' 으로 설정되어 있을 경우, Ctrl + C 로 프로그램 종료가 안됨
- DB 설정을 바꿀수 없는 상황이니, Ctrl + Z 로 프로그램을 나와서 프로세스는 직접 제거
$ kill -9 `ps -ef | grep "python bin/ks-manager.py" | grep -v grep | awk '{print $2}'`
"""

__appname__ = "kafka-subscriber"
__version__ = "1.0"


import optparse
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import ksm
from ksm.ks_manager import KSManager


if __name__ == "__main__":
    usage = """%prog [options]"""
    parser = optparse.OptionParser(usage=usage, description=__doc__)
    # parser.add_option(
    #     "--deal-date", metavar="DEAL_DATE", dest="deal_date", help="test request"
    # )

    ksm.add_basic_options(parser)
    (options, args) = parser.parse_args()

    config_dict = ksm.read_config_file(options.config_file)

    config_dict["app_name"] = __appname__
    log_dict = config_dict.get("log", {})
    log_file_name = "ksm.log"
    ksm.setup_logging(
        appname=__appname__,
        appvers=__version__,
        filename=log_file_name,
        dirname=options.log_dir,
        debug=options.debug,
        log_dict=log_dict,
        emit_platform_info=True,
    )

    ks_manager = KSManager()
    ks_manager.initialize(config_dict)
    ks_manager.run()
