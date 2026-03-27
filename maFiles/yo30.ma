behavior_name=yo
# Written by SFMC on UTC: 2026-03-25T18:23:13.808340
# yo30.ma

<start:b_arg>
	b_arg: start_when(enum)       2   # pitch idle (see doco below)
	b_arg: num_half_cycles_to_do(nodim) 2   # Number of dive/climbs to perform
	# arguments for dive_to
	b_arg: d_target_depth(m)     750.0
	b_arg: d_target_altitude(m)   49.0
	b_arg: d_use_pitch(enum)      3   # 1:battpos  2:setonce  3:servo
									#   in         rad        rad, <0 dive
	b_arg: d_pitch_value(X)   -0.4538 # -26.0 deg
	b_arg: d_use_bpump(enum)   2 # Buoyancy Pump absolute
	b_arg: d_bpump_value(X)   -430.0 # Dive buoyancy pump volume
	# arguments for climb_to
	b_arg: c_target_depth(m)      3.0
	b_arg: c_target_altitude(m)  -1
	b_arg: c_use_pitch(enum)      3   # 1:battpos  2:setonce  3:servo
									#   in         rad        rad, >0 climb
	b_arg: c_pitch_value(X)     0.6458 # 37.0 deg
	b_arg: c_use_bpump(enum)   2 # Buoyancy Pump absolute
	b_arg: c_bpump_value(X)   430.0 # Climb buoyancy pump volume
	b_arg: end_action(enum) 2     # 0-quit, 2 resume
<end:b_arg>
