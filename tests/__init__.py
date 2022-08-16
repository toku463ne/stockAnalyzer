import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import env
env.conf["is_test"] = True