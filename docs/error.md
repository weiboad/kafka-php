1. OFFSET_OUT_OF_RANGE 
// 将 offset 设置为当前分区的最开始或者最末端
// C

2. INVALID_MESSAGE
// retry
// C/P

3. UNKNOWN_TOPIC_OR_PARTITION
// retry
// 全部暂停重新开始
// C/P

4. INVALID_MESSAGE_SIZE
// retry
// C/P

5. LEADER_NOT_AVAILABLE
// retry
// C/P

6. NOT_LEADER_FOR_PARTITION
// retry
// 全部暂停重新开始
// C/P

7. REQUEST_TIMED_OUT
// retry
// C/P

8. BROKER_NOT_AVAILABLE
// retry
// 全部暂停重新开始
// C/P

9. REPLICA_NOT_AVAILABLE
// retry
// C/P

10. MESSAGE_SIZE_TOO_LARGE
// retry
// C/P

11. STALE_CONTROLLER_EPOCH
// retry
// C/P

12. OFFSET_METADATA_TOO_LARGE
// retry
// C

14. GROUP_LOAD_IN_PROGRESS
// retry
// 全部暂停重新开始
// C

15. GROUP_COORDINATOR_NOT_AVAILABLE
// retry
// 全部暂停重新开始
// C

16. NOT_COORDINATOR_FOR_GROUP
// retry
// 全部暂停重新开始
// C

17. INVALID_TOPIC
// retry
// 全部暂停重新开始
// C/P

18. RECORD_LIST_TOO_LARGE
// retry
// P

19. NOT_ENOUGH_REPLICAS
// error
// retry
// P

20. NOT_ENOUGH_REPLICAS_AFTER_APPEND
// error
// retry
// P

21 INVALID_REQUIRED_ACKS
// error
// retry
// P

22. ILLEGAL_GENERATION
// retry
// 全部暂停重新开始
// C

23. INCONSISTENT_GROUP_PROTOCOL
// error exit
// 全部暂停重新开始
// C

24. INVALID_GROUP_ID
// exit
// 全部暂停重新开始
// C

25. UNKNOWN_MEMBER_ID
// retry
// 全部暂停重新开始
// C

26. INVALID_SESSION_TIMEOUT
// retry
// 全部暂停重新开始
// C

27. REBALANCE_IN_PROGRESS
// retry 仅仅可以运行 heartbeat
// todo
// C

28. INVALID_COMMIT_OFFSET_SIZE
// retry  error
// C

29. TOPIC_AUTHORIZATION_FAILED
// error

30. GROUP_AUTHORIZATION_FAILED
// error

31. CLUSTER_AUTHORIZATION_FAILED
// error

43. UNSUPPORTED_FOR_MESSAGE_FORMAT
// error
