/*
 * Configure file of SSD-SMR cache system.
 * All the switch is turn-off by default. 
 */
/** configure of system structure **/
#undef NO_REAL_DISK_IO

#undef NO_CACHE

#undef CG_THROTTLE     // CGroup throttle.
#undef MULTIUSER

/**< Statistic information requirments defination */
#undef  LOG_ALLOW
#undef  LOG_SINGLE_REQ  // Print detail time information of each single request.

/** Simulator Related **/
#undef SIMULATION
#undef SIMULATOR_AIO
#undef SIMU_NO_DISK_IO

/** Daemon process **/
#define DAEMON_PROC
#define DAEMON_BANDWIDHT
#define DAEMON_CACHE_RUNTIME
#define DAEON_SMR_RUNTIME

