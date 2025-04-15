## Results

The real-time monitoring system successfully collected and processed sensor data from three key locations along the Rideau Canal Skateway. The system processed a total of 84 aggregated data points over 28 time windows.

### Aggregated Data Analysis

The Stream Analytics job aggregated data into 5-minute windows, calculating average values for key metrics. Sample aggregated output is shown below:

| Location | Time Window | Avg Ice Thickness (cm) | Max Snow Accumulation (cm) | Avg Surface Temp (�C) |
|----------|-------------|------------------------|----------------------------|------------------------|
| Dow's Lake | 2025-04-14 23:05:00+00:00 | 27.70 | 15.0 | -7.7 |
| Fifth Avenue | 2025-04-14 23:05:00+00:00 | 27.98 | 14.2 | -8.6 |
| NAC | 2025-04-14 23:05:00+00:00 | 27.40 | 12.9 | -8.9 |

### Key Findings

1. **Ice Thickness Patterns:**
   - Fifth Avenue maintained the thickest ice (average of 27.8 cm), approximately 1.0% thicker than Dow's Lake.
   - NAC showed the most variability in ice thickness over time.

2. **Temperature and Snow Relationships:**
   - Snow accumulation reached a maximum of 15.0 cm at Dow's Lake, which may require more frequent maintenance.
   - NAC had the lowest average surface temperature (-7.7�C).

3. **Temporal Patterns:**
   - Ice thickness measurements showed variations throughout the monitoring period.
   - The data enables NCC officials to make informed decisions about skateway safety.

![Ice Thickness Chart](../screenshots/ice_thickness_over_time.png)

The aggregated data enables NCC officials to make informed decisions about skateway safety and maintenance requirements at specific locations, enhancing both safety and visitor experience.
