package de.cyface.data;

public class Point3D {

    public static enum TypePoint3D {
        ACC, ROT, DIR
    }

    TypePoint3D type;

    final double x;
    final double y;
    final double z;
    final long timestamp;

    public Point3D(TypePoint3D type, double x, double y, double z, long timestamp) {
        this.type = type;
        this.x = x;
        this.y = y;
        this.z = z;
        this.timestamp = timestamp;
    }

    public final TypePoint3D getType() {
        return type;
    }

    public final double getX() {
        return x;
    }

    public final double getY() {
        return y;
    }

    public final double getZ() {
        return z;
    }

    public final long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("timestamp=").append(timestamp).append(",").append("x=").append(x).append(",").append("y=").append(y)
                .append(",").append("z=").append(z).append(",").append("sensortype=").append(type);
        return sb.toString();
    }

}
