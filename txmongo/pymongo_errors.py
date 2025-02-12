# Copied from pymongo/helpers.py:32 at commit d7d94b2776098dba32686ddf3ada1f201172daaf

# From the SDAM spec, the "node is shutting down" codes.
_SHUTDOWN_CODES = frozenset(
    [
        11600,  # InterruptedAtShutdown
        91,  # ShutdownInProgress
    ]
)
# From the SDAM spec, the "not master" error codes are combined with the
# "node is recovering" error codes (of which the "node is shutting down"
# errors are a subset).
_NOT_MASTER_CODES = (
    frozenset(
        [
            10058,  # LegacyNotPrimary <=3.2 "not primary" error code
            10107,  # NotMaster
            13435,  # NotMasterNoSlaveOk
            11602,  # InterruptedDueToReplStateChange
            13436,  # NotMasterOrSecondary
            189,  # PrimarySteppedDown
        ]
    )
    | _SHUTDOWN_CODES
)
# From the retryable writes spec.
_RETRYABLE_ERROR_CODES = _NOT_MASTER_CODES | frozenset(
    [
        7,  # HostNotFound
        6,  # HostUnreachable
        89,  # NetworkTimeout
        9001,  # SocketException
        262,  # ExceededTimeLimit
        134,  # ReadConcernMajorityNotAvailableYet
    ]
)

# From the transactions spec, all the retryable writes errors plus
# WriteConcernFailed.
_UNKNOWN_COMMIT_ERROR_CODES: frozenset = _RETRYABLE_ERROR_CODES | frozenset(
    [
        64,  # WriteConcernFailed
        50,  # MaxTimeMSExpired
    ]
)
