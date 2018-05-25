package de.cyface.data;

/**
 * This class encapsulates some constants required to serialize measurement data into the Cyface binary format.
 * 
 * @author Klemens Muthmann
 * @version 1.0.0
 * @since 2.0.0
 */
public final class ByteSizes {
    /**
     * Since our current API Level does not support <code>Long.Bytes</code>.
     */
    public final static int LONG_BYTES = Long.SIZE / 8;
    /**
     * Since our current API Level does not support <code>Integer.Bytes</code>.
     */
    public final static int INT_BYTES = Integer.SIZE / 8;
    /**
     * Since our current API Level does not support <code>Double.Bytes</code>.
     */
    public final static int DOUBLE_BYTES = Double.SIZE / 8;

    /**
     * A constant with the number of bytes for one uncompressed geo location entry in the Cyface binary format.
     */
    public final static int BYTES_IN_ONE_GEO_LOCATION_ENTRY = ByteSizes.LONG_BYTES + 3 * ByteSizes.DOUBLE_BYTES
            + ByteSizes.INT_BYTES;

    public final static int BYTES_IN_ONE_POINT_ENTRY = ByteSizes.LONG_BYTES + 3 * ByteSizes.DOUBLE_BYTES;
}