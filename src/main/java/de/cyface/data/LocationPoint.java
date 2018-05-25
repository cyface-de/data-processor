package de.cyface.data;

public class LocationPoint {

    final int accuracy;
    final double longitude;
    final double latitude;
    final double speed;
    final long timestamp;

    public LocationPoint(int accuracy, double longitude, double latitude, double speed, long timestamp) {
        this.accuracy = accuracy;
        this.longitude = longitude;
        this.latitude = latitude;
        this.speed = speed;
        this.timestamp = timestamp;
    }

    public final int getAccuracy() {
        return accuracy;
    }

    public final double getLongitude() {
        return longitude;
    }

    public final double getLatitude() {
        return latitude;
    }

    public final double getSpeed() {
        return speed;
    }

    public final long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("timestamp=").append(timestamp).append(",").append("lon=").append(longitude).append(",")
                .append("lat=").append(latitude).append(",").append("speed=").append(speed).append(",")
                .append("accuracy=").append(accuracy);
        return sb.toString();
    }

}
