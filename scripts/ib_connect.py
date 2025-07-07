# ib_connect.py

from ib_insync import IB
from config import IB_GATEWAY_HOST, IB_GATEWAY_PORT, IB_CLIENT_ID, logger

_ib_instance = None

def get_ib():

    global _ib_instance

    if _ib_instance is None or not _ib_instance.isConnected():
        _ib_instance = IB()

        try:
            _ib_instance.connect(IB_GATEWAY_HOST, IB_GATEWAY_PORT, clientId=IB_CLIENT_ID)
            logger.info("Connected to IB Gateway.")

        except Exception as e:
            logger.error(f"Failed to connect to IB Gateway: {e}")
            return None

    return _ib_instance