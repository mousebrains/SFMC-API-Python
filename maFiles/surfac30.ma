behavior_name=surface
# Written by SFMC on UTC: 2025-02-03T19:57:41.102255
# surfac30.ma
# J. Marquardt changed keystroke_wait_time from 300 to 599 sec
# BVW changed b_arg: when_secs(sec) from 1800 to 36000 (10 hours)

<start:b_arg>
	b_arg: start_when(enum) 12 # BAW_NOCOMM_SECS
	b_arg: when_secs(sec) 36000 # time in sec, will come to surface after reaching this
	b_arg: end_action(enum) 1 # Wait for Ctrl-C Quit/Resume
	b_arg: gps_wait_time(sec) 300 # Wait 300 seconds for gps
	b_arg: keystroke_wait_time(sec) 599 # Wait 300 seconds for control-C
	b_arg: c_use_pitch(enum) 3 # 3:servo
	b_arg: c_pitch_value(X) 0.4538 # 26 deg
	b_arg: printout_cycle_time(sec) 60.0 # How often to print dialog
<end:b_arg>
