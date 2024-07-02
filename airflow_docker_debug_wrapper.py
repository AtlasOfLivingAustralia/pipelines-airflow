#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import os
import re
import sys

from airflow.__main__ import main


def launch_debugger():
    # Allow debugging with VSCode: breakpoints, debug console, etc.
    if bool(int(os.environ.get('DEBUGGER', 0))):
        import multiprocessing
        if multiprocessing.current_process().pid > 1:
            import debugpy

            # Listen on 10001/tcp
            debugpy.listen(("0.0.0.0", 9034))
            print("VS Code debugger can now be attached, press F5 in VS Code", flush=True)
            # Wait for VSCode to attach the session
            print("VS Code debugger attached, enjoy debugging", flush=True)

if __name__ == '__main__':
    print(sys.argv)
    if ' '.join(sys.argv[1:3]) == "celery worker": launch_debugger()
    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
    sys.exit(main())