package de.cyface.dataprocessor;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipException;

import org.apache.commons.io.IOUtils;

import de.cyface.data.ByteSizes;
import de.cyface.data.LocationPoint;
import de.cyface.data.Point3D;
import de.cyface.data.Point3D.TypePoint3D;

public abstract class AbstractCyfaceDataProcessor implements ICyfaceDataProcessor {

    static final String uncompress_FIRST_EXCEPTION = "Binary has to be uncompressed before other operations can be used.";
    static final String PREPARE_FIRST_EXCEPTION = "Binary has to be prepared before this operations can be used.";
    static final int DEFAULT_INFLATER_BYTE_BUF = 4096;

    protected boolean uncompressed = false;
    protected boolean prepared = false;
    private CyfaceBinaryHeader header;
    private InputStream binaryInputStream;
    protected InputStream uncompressedBinaryInputStream;
    protected OutputStream uncompressedBinaryOutputStream;

    protected InputStream compressedBinaryInputStream;

    private InflaterInputStream inflaterInputStream;

    public AbstractCyfaceDataProcessor(InputStream binaryInputStream, boolean compressed) {
        Objects.requireNonNull(binaryInputStream, "InputStream must not be null.");
        uncompressed = !compressed;
        this.binaryInputStream = binaryInputStream;
    }

    @Override
    public boolean isUncompressed() {
        return uncompressed;
    }

    @Override
    public boolean isPrepared() {
        return prepared;
    }

    @Override
    public CyfaceBinaryHeader getHeader() throws CyfaceCompressedDataProcessorException, IOException {
        if (header == null) {
            this.readHeader();
        }

        return header;
    }

    @Override
    public ICyfaceDataProcessor uncompressAndPrepare() throws IOException, CyfaceCompressedDataProcessorException {
        uncompress();
        prepare();
        return this;
    }

    protected abstract InputStream getCompressedInputStream();

    protected abstract InputStream getUncompressedInputStream();

    @Override
    public ICyfaceDataProcessor uncompress() throws CyfaceCompressedDataProcessorException, IOException {
        InputStream reader = null;
        if (!uncompressed) {
            boolean nowrap = false;
            boolean retry = true;

            while (retry && !uncompressed) {
                reader = getCompressedInputStream();
                this.compressedBinaryInputStream = new BufferedInputStream(reader);
                try {
                    uncompress(compressedBinaryInputStream, uncompressedBinaryOutputStream, nowrap);
                    uncompressed = true;
                    retry = false;
                } catch (ZipException e1) {
                    // binary input could be created with iOS, retry with nowrap option
                    if (e1.getMessage().equals("incorrect header check")) {
                        nowrap = true;
                    } else {
                        retry = false;
                        throw new CyfaceCompressedDataProcessorException(
                                "Binary input could not be uncompressed: " + e1.getMessage());
                    }
                } finally {
                    if (reader != null) {
                        reader.close();
                    }
                }
            }

            // close streams after write out is done
            uncompressedBinaryOutputStream.flush();
            uncompressedBinaryOutputStream.close();
            compressedBinaryInputStream.close();
            inflaterInputStream.close();

            uncompressedBinaryInputStream = getUncompressedInputStream();
        } else {
            reader = getCompressedInputStream();
            IOUtils.copy(reader, uncompressedBinaryOutputStream, 1024);
            uncompressedBinaryInputStream = getUncompressedInputStream();
            if (reader != null) {
                reader.close();
            }
        }

        return this;
    }

    protected void uncompress(InputStream compressedBinaryInputStream, OutputStream uncompressedBinaryOutputStream,
            boolean nowrap) throws CyfaceCompressedDataProcessorException, IOException {

        Inflater uncompressor = new Inflater(nowrap);
        this.inflaterInputStream = new InflaterInputStream(compressedBinaryInputStream, uncompressor,
                DEFAULT_INFLATER_BYTE_BUF);

        IOUtils.copy(inflaterInputStream, uncompressedBinaryOutputStream, 4096);
    }

    /**
     * Except for the header, split each part of the uncompressed input binary to a separate bin for easy separate
     * access of arbitrary sensor data. The implementation depends on the type of CyfaceDataProcessor (e.g. file system
     * or in-memory).
     * 
     * @throws IOException
     * @throws CyfaceCompressedDataProcessorException
     */
    protected abstract void prepare() throws CyfaceCompressedDataProcessorException, IOException;

    /**
     * Deserializes a single geo location from an array of bytes in Cyface binary format.
     *
     * @param bytes The bytes array to deserialize the geo location from.
     * @return LocationPoint - Each LocationPoint contains 5 entrys keyed with "timestamp",
     *         "lat", "lon", "speed" and "accuracy" with the appropriate values. The timestamp is a <code>long</code>,
     *         accuracy is an <code>int</code> and all other values are <code>double</code> values.
     */
    protected LocationPoint deserializeGeoLocation(final byte[] bytes) {
        LocationPoint locPoint = null;

        for (int i = 0; i < bytes.length; i += ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY) {
            ByteBuffer buffer = ByteBuffer
                    .wrap(Arrays.copyOfRange(bytes, i, i + ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY));

            // readout order is important, its a byte buffer dude
            long timestamp = buffer.order(ByteOrder.BIG_ENDIAN).getLong();
            double latitude = buffer.order(ByteOrder.BIG_ENDIAN).getDouble();
            double longitude = buffer.order(ByteOrder.BIG_ENDIAN).getDouble();
            double speed = buffer.order(ByteOrder.BIG_ENDIAN).getDouble();
            int accuracy = buffer.order(ByteOrder.BIG_ENDIAN).getInt();

            locPoint = new LocationPoint(accuracy, longitude, latitude, speed, timestamp);

        }
        return locPoint;
    }

    /**
     * Deserializes a single 3D sample point (i.e. acceleration, rotation or direction) from an array of bytes in
     * Cyface binary format.
     *
     * @param bytes The bytes array to deserialize the sample points from.
     * @param type the sensor type from which the given sample Point was recorded
     * @return Point3D - Each Point3D contains a timestamp as <code>long</code>, sensor values x,y,z are
     *         <code>double</code> and a sensor type <code>TypePoint3D</code>.
     */
    protected Point3D deserializePoint3D(final byte[] bytes, final TypePoint3D type) {
        Point3D point3D = null;

        for (int i = 0; i < bytes.length; i += ByteSizes.BYTES_IN_ONE_POINT_ENTRY) {
            ByteBuffer buffer = ByteBuffer.wrap(Arrays.copyOfRange(bytes, i, i + ByteSizes.BYTES_IN_ONE_POINT_ENTRY));

            // readout order is important, its a byte buffer dude
            long timestamp = buffer.order(ByteOrder.BIG_ENDIAN).getLong();
            double x = buffer.order(ByteOrder.BIG_ENDIAN).getDouble();
            double y = buffer.order(ByteOrder.BIG_ENDIAN).getDouble();
            double z = buffer.order(ByteOrder.BIG_ENDIAN).getDouble();

            point3D = new Point3D(type, x, y, z, timestamp);
        }

        return point3D;
    }

    /**
     * Copies a part (bytes from start to end) of an input stream to an output stream (thx to
     * https://stackoverflow.com/questions/22645895/java-copy-part-of-inputstream-to-outputstream)
     * 
     * @param input
     * @param output
     * @param start
     * @param end
     * @throws IOException
     */
    protected static void copyStream(final InputStream input, final OutputStream output, final long start,
            final long end) throws IOException {
        for (int i = 0; i < start; i++)
            input.read(); // dispose of the unwanted bytes
        int bufferSize = 4096;
        byte[] buffer = new byte[bufferSize]; // Adjust if you want
        int bytesRead;
        long totalRead = 0;
        boolean done = false;
        while (!done) // test for EOF or end reached
        {
            if ((totalRead + bufferSize) >= end) {
                int lastReadBuf = (int)(end - totalRead);
                byte[] lastBuf = new byte[lastReadBuf];
                output.write(lastBuf, 0, input.read(lastBuf));
                done = true;
                totalRead += lastReadBuf;
            } else {
                bytesRead = input.read(buffer);
                output.write(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }
            output.flush();
        }
    }

    protected Point3D pollNext3DPoint(final BufferedInputStream bufInputStream, final TypePoint3D type)
            throws IOException {
        byte[] point3DBytes = new byte[ByteSizes.BYTES_IN_ONE_POINT_ENTRY];
        int read = bufInputStream.read(point3DBytes, 0, ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        if (read != -1) {
            return deserializePoint3D(point3DBytes, type);
        } else {
            return null;
        }
    }

    protected static final class CyfaceCompressedDataProcessorException extends Exception {

        public CyfaceCompressedDataProcessorException(final String message) {
            super(message);
        }

        /**
         * 
         */
        private static final long serialVersionUID = 3893090953252038103L;

    }

    private void readHeader() throws CyfaceCompressedDataProcessorException, IOException {
        checkUncompressedOrThrowException();
        final byte[] individualBytes = new byte[18];
        uncompressedBinaryInputStream.read(individualBytes, 0, 18);

        ByteBuffer buffer = ByteBuffer.wrap(individualBytes);
        this.header = new CyfaceBinaryHeader();
        header.setFormatVersion(buffer.order(ByteOrder.BIG_ENDIAN).getShort(0));
        header.setNumberOfGeoLocations(buffer.order(ByteOrder.BIG_ENDIAN).getInt(2));
        header.setNumberOfAccelerations(buffer.order(ByteOrder.BIG_ENDIAN).getInt(6));
        header.setNumberOfRotations(buffer.order(ByteOrder.BIG_ENDIAN).getInt(10));
        header.setNumberOfDirections(buffer.order(ByteOrder.BIG_ENDIAN).getInt(14));
        header.setBeginOfGeoLocationsIndex(18);
        header.setBeginOfAccelerationsIndex(header.getBeginOfGeoLocationsIndex()
                + header.getNumberOfGeoLocations() * ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY);
        header.setBeginOfRotationsIndex(header.getBeginOfAccelerationsIndex()
                + header.getNumberOfAccelerations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        header.setBeginOfDirectionsIndex(
                header.getBeginOfRotationsIndex() + header.getNumberOfRotations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
    }

    protected void checkUncompressedOrThrowException() throws CyfaceCompressedDataProcessorException {
        if (!uncompressed) {
            throw new CyfaceCompressedDataProcessorException(uncompress_FIRST_EXCEPTION);
        }
    }

    protected void checkPreparedOrThrowException() throws CyfaceCompressedDataProcessorException {
        if (!prepared) {
            throw new CyfaceCompressedDataProcessorException(uncompress_FIRST_EXCEPTION);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            closeStreamIfNotNull(inflaterInputStream);
            closeStreamIfNotNull(compressedBinaryInputStream);
            closeStreamIfNotNull(uncompressedBinaryInputStream);
            closeStreamIfNotNull(uncompressedBinaryOutputStream);
        } catch (IOException e) {
            throw new RuntimeException("Could not close Stream, while trying to close DataProcessor.", e);
        }
    }

    protected void closeStreamIfNotNull(Closeable closeable) throws IOException {
        if (closeable != null) {
            closeable.close();
        }
    }

}
