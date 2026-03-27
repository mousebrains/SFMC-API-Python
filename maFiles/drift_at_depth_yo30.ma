behavior_name=yo
# Written by SFMC on UTC: 2026-03-25T18:23:13.808340
# yo30.ma

<start:b_arg>
	# Arguments for drift_at_depth
    b_arg: start_when(enum) 4                      #! choices=start_when([0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 13])
    b_arg: when_secs(sec)   180    # For start_when = 6, 9, or 10
    b_arg: when_wpt_dist(m) 10                     # ! min = 5.0
	
    b_arg: end_action(enum) 2      # 0-quit, 2-resume
    b_arg: stop_when_hover_for(sec) 7200.0 # terminate hover when depth does not change for
                                           # this many secs, <0 to disable
    b_arg: est_time_to_settle(s) 360.0 # Used to force invalid cc_time_til_inflect for this
                                       # This many seconds at the beginning of the behavior.
    b_arg: target_depth(m)       200.0 # depth to drift at
    b_arg: target_altitude(m)     50.0 # altitude to drift at, <=0 disables
    b_arg: target_deadband(m)      5.0 # +/- around target depth or altitude
    b_arg: depth_ctrl(enum)          2 # 2: Recommended mode for thruster control. pitch-based depth control: see 'pitching depth control mode' sensors
    b_arg: use_pitch(enum) 3       # 3  Servo on Pitch
    b_arg: pitch_value(X)  ??     # use_pitch == 4    cc, clips to max legal, >0 to nose down
                                   # use_pitch == 2,3  rad, desired pitch angle, <0 to dive
                                   # use_pitch == 1    in,  desired battpos, >0 to nose down
                                   #                     clips to max legal
    b_arg: use_thruster(enum)   4               # 4  Command input power. See sensors for use_thruster = power
    b_arg: thruster_value(X)     3              # use_thruster == 4  watt, desired input power, between [1, 9] Watts
    b_arg: enable_steering(bool) 0   # Enable or disable steering while hovering. If True, heading is
                                     # controlled as normal (set_heading, goto_list etc) during hovering. If False,
                                     # commanded fin position = 0 during hovering.
	
	# Arguments for dive_to when diving to hover zone
    b_arg: d_use_bpump(enum)       2            # 2  Buoyancy Pump absolute (uses d_bpump_value as total difference between dive and climbs)
    b_arg: d_bpump_value(X)  -430.0
    b_arg: d_use_pitch(enum)       3  # servo on pitch
    b_arg: d_pitch_value(X)  -0.4538  # ~-26 degrees
    b_arg: d_use_thruster(enum)   4               # 4  Command input power. See sensors for use_thruster = power
    b_arg: d_thruster_value(X)   3              # use_thruster == 4  watt, desired input power, between [1, 9] Watts
												
	# Arguments for climb_to when climbing to hover zone
    b_arg: c_use_bpump(enum)       2            # 2  Buoyancy Pump absolute (uses bpump_value as total difference between dive and climbs)
    b_arg: c_bpump_value(X)   430.0
    b_arg: c_use_pitch(enum)       3  # servo on pitch
    b_arg: c_pitch_value(X)   0.6545 # 37.5 deg
    b_arg: c_use_thruster(enum)   4               # 4  Command input power. See sensors for use_thruster = power
    b_arg: c_thruster_value(X)   3              # use_thruster == 4  watt, desired input power, between [1, 9] Watts
<end:b_arg>
