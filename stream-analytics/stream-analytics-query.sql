-- Aggregate data for each location over a 5-minute tumbling window
SELECT
    location,
    System.Timestamp() AS windowEndTime,
    AVG(iceThickness) AS avgIceThickness,
    MAX(snowAccumulation) AS maxSnowAccumulation,
    AVG(surfaceTemperature) AS avgSurfaceTemperature,
    AVG(externalTemperature) AS avgExternalTemperature,
    COUNT(*) AS numberOfReadings
INTO
    [rideau-canal-output]
FROM
    [rideau-canal-input]
GROUP BY
    location,
    TumblingWindow(minute, 5)
