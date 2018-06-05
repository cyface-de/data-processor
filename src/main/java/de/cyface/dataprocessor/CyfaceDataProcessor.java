package de.cyface.dataprocessor;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipException;

import org.apache.commons.io.IOUtils;

import de.cyface.data.ByteSizes;
import de.cyface.data.LocationPoint;
import de.cyface.data.Point3D;
import de.cyface.data.Point3D.TypePoint3D;

/**
 * The CyfaceDataProcessor can be used to easily get Strings of human readable sensor data from the Cyface binary
 * format, either plain (.cyf) or compressed (.ccyf). It is optimized to use as less memory as possible. Therefore the
 * local file system is utilized to create binary temp files for each sensor. Several functions allow to poll single
 * sensor values from those files as {@link LocationPoint} or {@link Point3D} or objects, which contain the human
 * readable data for further processing.
 * 
 * @author Philipp Grubitzsch
 *
 */
public class CyfaceDataProcessor implements Closeable {

    static final int DEFAULT_INFLATER_BYTE_BUF = 4096;
    static final String uncompress_FIRST_EXCEPTION = "Binary has to be uncompressed before other operations can be used.";
    static final String PREPARE_FIRST_EXCEPTION = "Binary has to be prepared before this operations can be used.";

    static final String TEMP_FOLDER = "uncompressed-temp/";

    InputStream binaryInputStream;
    InputStream compressedBinaryInputStream;
    FileOutputStream uncompressedBinaryOutputStream;
    BufferedInputStream uncompressedBinaryInputStream;
    InflaterInputStream inflaterInputStream;

    // separate temporary file parts for each sensor type
    File tempLocFile;
    File tempAccFile;
    File tempRotFile;
    File tempDirFile;

    File uncompressedTempfile;

    private boolean uncompressed = false;
    private boolean prepared = false;

    /**
     * 
     * @return True, if the data is already uncompressed.
     */
    public boolean isUncompressed() {
        return uncompressed;
    }

    /**
     * 
     * @return True, if input is completely prepare for read out of sensor data. This include optional necessary
     *         uncompression, header readout and sensor data split.
     */
    public boolean isPrepared() {
        return prepared;
    }

    private CyfaceBinaryHeader header;

    /**
     * Constructor for the Processor
     * 
     * @param binaryInputStream the binary input either compressed or uncompressed
     * @param compressed flag to tell the processor if the binary input is compressed
     * @throws IOException
     */
    public CyfaceDataProcessor(InputStream binaryInputStream, boolean compressed) throws IOException {
        File tempFolder = new File(TEMP_FOLDER);
        if (!tempFolder.exists()) {
            tempFolder.mkdirs();
        }
        this.uncompressedTempfile = new File(TEMP_FOLDER + UUID.randomUUID().toString());
        this.uncompressedBinaryOutputStream = new FileOutputStream(uncompressedTempfile);
        uncompressed = !compressed;
        this.binaryInputStream = binaryInputStream;
    }

    /**
     * 
     * @return byte array for in memory processing. small binaries only!
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public byte[] getUncompressedBinaryAsArray() throws CyfaceCompressedDataProcessorException, IOException {
        checkUncompressedOrThrowException();
        return Files.readAllBytes(uncompressedTempfile.toPath());
    }

    /**
     * This method uncompress and prepare data to easily access arbitrary sensor data.
     * 
     * @return The instance of this specific Processor for fluently usage
     * @throws IOException
     * @throws CyfaceCompressedDataProcessorException
     */
    public CyfaceDataProcessor uncompressAndPrepare() throws IOException, CyfaceCompressedDataProcessorException {
        uncompress();
        prepare();
        return this;
    }

    /**
     * This method uncompress binary data.
     * 
     * @return The instance of this specific Processor for fluently usage
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public CyfaceDataProcessor uncompress() throws CyfaceCompressedDataProcessorException, IOException {
        if (!uncompressed) {
            boolean nowrap = false;
            boolean retry = true;

            while (retry && !uncompressed) {
                this.compressedBinaryInputStream = new BufferedInputStream(binaryInputStream);
                try {
                    uncompress(nowrap);
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
                }
            }
            // close streams after write out is done
            uncompressedBinaryOutputStream.close();
            compressedBinaryInputStream.close();
            inflaterInputStream.close();

            uncompressedBinaryInputStream = new BufferedInputStream(new FileInputStream(uncompressedTempfile));
        } else {
            IOUtils.copy(binaryInputStream, uncompressedBinaryOutputStream, 1024);
            uncompressedBinaryInputStream = new BufferedInputStream(new FileInputStream(uncompressedTempfile));
        }

        return this;
    }

    private void uncompress(boolean nowrap) throws CyfaceCompressedDataProcessorException, IOException {

        Inflater uncompressor = new Inflater(nowrap);
        this.inflaterInputStream = new InflaterInputStream(compressedBinaryInputStream, uncompressor,
                DEFAULT_INFLATER_BYTE_BUF);

        IOUtils.copy(inflaterInputStream, uncompressedBinaryOutputStream, 4096);

    }

    BufferedInputStream tempLocStream;

    /**
     * Polls the next available geo location from binary temp file
     * 
     * @return an Location Point or null, if all entries have been already read.
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public LocationPoint pollNextLocationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
        if (tempLocStream == null) {
            tempLocStream = new BufferedInputStream(new FileInputStream(tempLocFile),
                    ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY);
        }
        byte[] locationBytes = new byte[ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY];
        int read = tempLocStream.read(locationBytes, 0, ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY);
        if (read != -1) {
            return deserializeGeoLocation(locationBytes);
        } else {
            tempLocStream.close();
            return null;
        }
    }

    BufferedInputStream tempAccStream;

    /**
     * Polls the next available acceleration point from binary temp file
     * 
     * @return an acceleration point or null, if all entries have been already read.
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public Point3D pollNextAccelerationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
        // no acc data
        if (tempAccFile == null) {
            return null;
        }
        if (tempAccStream == null) {
            tempAccStream = new BufferedInputStream(new FileInputStream(tempAccFile),
                    ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        }

        Point3D nextPoint = pollNext3DPoint(tempAccStream, TypePoint3D.ACC);
        if (nextPoint == null) {
            tempAccStream.close();
        }
        return nextPoint;
    }

    BufferedInputStream tempRotStream;

    /**
     * Polls the next available rotation point from binary temp file
     * 
     * @return an rotation point or null, if all entries have been already read.
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public Point3D pollNextRotationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
        // no rot data
        if (tempRotFile == null) {
            return null;
        }
        if (tempRotStream == null) {
            tempRotStream = new BufferedInputStream(new FileInputStream(tempRotFile),
                    ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        }

        Point3D nextPoint = pollNext3DPoint(tempRotStream, TypePoint3D.ROT);
        if (nextPoint == null) {
            tempRotStream.close();
        }
        return nextPoint;
    }

    BufferedInputStream tempDirStream;

    /**
     * Polls the next available direction point from binary temp file
     * 
     * @return an direction point or null, if all entries have been already read.
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public Point3D pollNextDirectionPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
        // no dir data
        if (tempDirFile == null) {
            return null;
        }
        if (tempDirStream == null) {
            tempDirStream = new BufferedInputStream(new FileInputStream(tempDirFile),
                    ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        }

        Point3D nextPoint = pollNext3DPoint(tempDirStream, TypePoint3D.DIR);
        if (nextPoint == null) {
            tempDirStream.close();
        }
        return nextPoint;
    }

    private Point3D pollNext3DPoint(BufferedInputStream bufInputStream, TypePoint3D type) throws IOException {
        byte[] point3DBytes = new byte[ByteSizes.BYTES_IN_ONE_POINT_ENTRY];
        int read = bufInputStream.read(point3DBytes, 0, ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        if (read != -1) {
            return deserializePoint3D(point3DBytes, type);
        } else {
            return null;
        }
    }

    private void readHeader() throws CyfaceCompressedDataProcessorException, IOException {
        checkUncompressedOrThrowException();
        byte[] individualBytes = new byte[18];
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

    /**
     * Except for the header, split each part of the binary to a separate bin file for easy separate access of arbitrary
     * sensor data.
     * 
     * @throws IOException
     * @throws CyfaceCompressedDataProcessorException
     */
    private void prepare() throws CyfaceCompressedDataProcessorException, IOException {
        // read the header first.
        getHeader();

        // write out geo locations
        tempLocFile = new File(uncompressedTempfile + "_loc");
        FileOutputStream binLocationTemp = new FileOutputStream(tempLocFile);
        int locationBytesCount = header.getNumberOfGeoLocations() * ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY;
        copyStream(uncompressedBinaryInputStream, binLocationTemp, 0, locationBytesCount);
        binLocationTemp.close();

        // write out accelerometer data
        if (header.getNumberOfAccelerations() > 0) {

            tempAccFile = new File(uncompressedTempfile + "_acc");
            FileOutputStream binAccTemp = new FileOutputStream(tempAccFile);
            int accBytesCount = header.getNumberOfAccelerations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY;
            copyStream(uncompressedBinaryInputStream, binAccTemp, 0, accBytesCount);
            binAccTemp.close();
        }

        // write out rotation data
        if (header.getNumberOfRotations() > 0) {
            tempRotFile = new File(uncompressedTempfile + "_rot");
            FileOutputStream binRotTemp = new FileOutputStream(tempRotFile);
            int rotBytesCount = header.getNumberOfRotations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY;
            copyStream(uncompressedBinaryInputStream, binRotTemp, 0, rotBytesCount);
            binRotTemp.close();
        }

        // write out direction data
        if (header.getNumberOfDirections() > 0) {
            tempDirFile = new File(uncompressedTempfile + "_dir");
            FileOutputStream binDirTemp = new FileOutputStream(tempDirFile);
            int dirBytesCount = ByteSizes.BYTES_IN_ONE_POINT_ENTRY * header.getNumberOfDirections();
            copyStream(uncompressedBinaryInputStream, binDirTemp, 0, dirBytesCount);
            binDirTemp.close();
        }

        // close input stream
        uncompressedBinaryInputStream.close();

        prepared = true;
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
    private static void copyStream(InputStream input, OutputStream output, long start, long end) throws IOException {
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
        }
    }

    /**
     * Get the Header. Requires isUncompressed() true.
     * 
     * @return the CyfaceBinaryHeader
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public CyfaceBinaryHeader getHeader() throws CyfaceCompressedDataProcessorException, IOException {
        if (header == null) {
            this.readHeader();
        }

        return header;
    }

    private void checkUncompressedOrThrowException() throws CyfaceCompressedDataProcessorException {
        if (!uncompressed) {
            throw new CyfaceCompressedDataProcessorException(uncompress_FIRST_EXCEPTION);
        }
    }

    private void checkPreparedOrThrowException() throws CyfaceCompressedDataProcessorException {
        if (!prepared) {
            throw new CyfaceCompressedDataProcessorException(uncompress_FIRST_EXCEPTION);
        }
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
    private Point3D deserializePoint3D(byte[] bytes, TypePoint3D type) {
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
     * Deserializes a single geo location from an array of bytes in Cyface binary format.
     *
     * @param bytes The bytes array to deserialize the geo location from.
     * @return LocationPoint - Each LocationPoint contains 5 entrys keyed with "timestamp",
     *         "lat", "lon", "speed" and "accuracy" with the appropriate values. The timestamp is a <code>long</code>,
     *         accuracy is an <code>int</code> and all other values are <code>double</code> values.
     */
    private LocationPoint deserializeGeoLocation(byte[] bytes) {
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

    protected static final class CyfaceCompressedDataProcessorException extends Exception {

        public CyfaceCompressedDataProcessorException(String message) {
            super(message);
        }

        /**
         * 
         */
        private static final long serialVersionUID = 3893090953252038103L;

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        try {
            // if given uncompressed, it can be null
            binaryInputStream.close();
            if (compressedBinaryInputStream != null) {
                compressedBinaryInputStream.close();
            }
            // if given uncompressed, it can be null
            if (inflaterInputStream != null) {
                inflaterInputStream.close();
            }
            uncompressedBinaryInputStream.close();
            uncompressedBinaryOutputStream.close();
            tempLocStream.close();
            tempAccStream.close();
            tempRotStream.close();
            tempDirStream.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("Could not close Stream, while trying to close DataProcessor.", e);
        }

        if (!(uncompressedTempfile.delete() && tempLocFile.delete() && tempAccFile.delete() && tempRotFile.delete()
                && tempDirFile.delete())) {
            throw new RuntimeException("Could not delete all tempfiles.");
        }

    }

}
