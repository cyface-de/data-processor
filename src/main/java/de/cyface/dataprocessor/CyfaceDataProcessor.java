package de.cyface.dataprocessor;

import java.io.BufferedInputStream;
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

import org.apache.commons.io.IOUtils;

import de.cyface.data.ByteSizes;
import de.cyface.data.LocationPoint;
import de.cyface.data.Point3D;
import de.cyface.data.Point3D.TypePoint3D;

/**
 * The CyfaceDataProcessor can be used to easily get Strings of human readable sensor data from the Cyface binary format
 * plain (.cyf) or compressed (.ccyf). It is optimized to use as less memory as possible. Therefore the local
 * file system is utilized to create binary temp files for each sensor. Several functions allow to poll single sensor
 * values from those files as Map<String,?> Objects, which contain the human readable data for further processing.
 * 
 * @author Philipp Grubitzsch
 *
 */
public class CyfaceDataProcessor {

    static final int DEFAULT_INFLATER_BYTE_BUF = 1024;
    static final String DECOMPRESS_FIRST_EXCEPTION = "Binary has to be decompressed before other operations can be used.";
    static final String PREPARE_FIRST_EXCEPTION = "Binary has to be prepared before this operations can be used.";
    static final String NOT_SO_MANY_GEOLOCATIONS_EXCEPTION = "The requested number of geolocations is higher than the available geolocations. Read number of available geolocations from Header first.";

    static final String TEMP_FOLDER = "decompressed-temp/";

    InputStream compressedBinaryInputStream;
    FileOutputStream decompressedBinaryOutputStream;
    BufferedInputStream decompressedBinaryInputStream;
    InflaterInputStream inflaterInputStream;

    // separate temporary file parts for each sensor type
    File tempLocFile;
    File tempAccFile;
    File tempRotFile;
    File tempDirFile;

    File decompressedTempfile;

    private boolean decompressed = false;
    private boolean prepared = false;

    /**
     * 
     * @return True, if the data is already decompressed.
     */
    public boolean isDecompressed() {
        return decompressed;
    }

    /**
     * 
     * @return True, if input is completely prepare for read out of sensor data. This include optional necessary
     *         decompression, header readout and sensor data split.
     */
    public boolean isPrepared() {
        return prepared;
    }

    private CyfaceBinaryHeader header;

    /**
     * Constructor for the Processor
     * 
     * @param binaryInputStream the binary input either compressed or decompressed
     * @param compressed flag to tell the processor if the binary input is compressed
     * @throws IOException
     */
    public CyfaceDataProcessor(InputStream binaryInputStream, boolean compressed) throws IOException {
        this.decompressedTempfile = new File(TEMP_FOLDER + UUID.randomUUID().toString());
        this.decompressedBinaryOutputStream = new FileOutputStream(decompressedTempfile);
        if (!compressed) {
            decompressed = true;
            IOUtils.copy(binaryInputStream, decompressedBinaryOutputStream, 1024);
            decompressedBinaryInputStream = new BufferedInputStream(new FileInputStream(decompressedTempfile));
        } else {
            this.compressedBinaryInputStream = binaryInputStream;
        }
        File tempFolder = new File(TEMP_FOLDER);
        if (!tempFolder.exists()) {
            tempFolder.mkdirs();
        }
    }

    /**
     * 
     * @return byte array for in memory processing. small binaries only!
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public byte[] getDecompressedBinaryAsArray() throws CyfaceCompressedDataProcessorException, IOException {
        checkDecompressedOrThrowException();
        return Files.readAllBytes(decompressedTempfile.toPath());
    }

    /**
     * This method decompress and prepare data to easily access arbitrary sensor data.
     * 
     * @return The instance of this specific Processor for fluently usage
     * @throws IOException
     * @throws CyfaceCompressedDataProcessorException
     */
    public CyfaceDataProcessor decompressAndPrepare() throws IOException, CyfaceCompressedDataProcessorException {
        decompress();
        prepare();
        return this;
    }

    public CyfaceDataProcessor decompress() throws CyfaceCompressedDataProcessorException, IOException {
        if (!decompressed) {
            Inflater decompressor = new Inflater(true);
            this.inflaterInputStream = new InflaterInputStream(compressedBinaryInputStream, decompressor,
                    DEFAULT_INFLATER_BYTE_BUF);

            IOUtils.copy(inflaterInputStream, decompressedBinaryOutputStream, 1024);

            // close streams after write out is done
            decompressedBinaryOutputStream.close();
            compressedBinaryInputStream.close();
            inflaterInputStream.close();
            decompressed = true;
            decompressedBinaryInputStream = new BufferedInputStream(new FileInputStream(decompressedTempfile));
        }
        return this;
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

    public Point3D pollNextRotationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
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

    public Point3D pollNextDirectionPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
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
        checkDecompressedOrThrowException();
        byte[] individualBytes = new byte[18];
        decompressedBinaryInputStream.read(individualBytes, 0, 18);

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
        this.readHeader();

        // write out geo locations
        tempLocFile = new File(decompressedTempfile + "_loc");
        FileOutputStream binLocationTemp = new FileOutputStream(tempLocFile);
        int locationBytesCount = header.getNumberOfGeoLocations() * ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY;
        System.out.println(locationBytesCount);
        copyStream(decompressedBinaryInputStream, binLocationTemp, 0, locationBytesCount);
        binLocationTemp.close();

        // write out accelerometer data
        tempAccFile = new File(decompressedTempfile + "_acc");
        FileOutputStream binAccTemp = new FileOutputStream(tempAccFile);
        int accBytesCount = header.getNumberOfAccelerations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY;
        copyStream(decompressedBinaryInputStream, binAccTemp, 0, accBytesCount);
        binAccTemp.close();

        // write out rotation data
        tempRotFile = new File(decompressedTempfile + "_rot");
        FileOutputStream binRotTemp = new FileOutputStream(tempRotFile);
        int rotBytesCount = header.getNumberOfRotations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY;
        copyStream(decompressedBinaryInputStream, binRotTemp, 0, rotBytesCount);
        binRotTemp.close();

        // write out direction data
        tempDirFile = new File(decompressedTempfile + "_dir");
        FileOutputStream binDirTemp = new FileOutputStream(tempDirFile);
        int dirBytesCount = ByteSizes.BYTES_IN_ONE_POINT_ENTRY * header.getNumberOfDirections();
        copyStream(decompressedBinaryInputStream, binRotTemp, 0, dirBytesCount);
        binDirTemp.close();

        // close input stream
        decompressedBinaryInputStream.close();

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
            System.out.println(totalRead);

        }
    }

    public CyfaceBinaryHeader getHeader() throws CyfaceCompressedDataProcessorException, IOException {
        if (header == null) {
            readHeader();
        }

        return header;
    }

    private void checkDecompressedOrThrowException() throws CyfaceCompressedDataProcessorException {
        if (!decompressed) {
            throw new CyfaceCompressedDataProcessorException(DECOMPRESS_FIRST_EXCEPTION);
        }
    }

    private void checkPreparedOrThrowException() throws CyfaceCompressedDataProcessorException {
        if (!prepared) {
            throw new CyfaceCompressedDataProcessorException(DECOMPRESS_FIRST_EXCEPTION);
        }
    }

    private void checkHeaderRead() throws CyfaceCompressedDataProcessorException, IOException {
        if (header == null) {
            readHeader();
        }
    }

    /**
     * Deserializes a single 3D sample point (i.e. acceleration, rotation or direction) from an array of bytes in
     * Cyface binary format.
     *
     * @param bytes The bytes array to deserialize the sample points from.
     * @return A poor mans list of objects (i.e. <code>Map</code>). Each map contains 4 entrys for x, y, z and timestamp
     *         with the corresponding values. A timestamp is a <code>long</code>, all other values are
     *         <code>double</code>.
     */
    private Point3D deserializePoint3D(byte[] bytes, TypePoint3D type) {
        Point3D point3D = null;

        for (int i = 0; i < bytes.length; i += ByteSizes.BYTES_IN_ONE_POINT_ENTRY) {
            ByteBuffer buffer = ByteBuffer.wrap(Arrays.copyOfRange(bytes, i, i + ByteSizes.BYTES_IN_ONE_POINT_ENTRY));

            // readout order ist important, its a byte buffer dude
            long timestamp = buffer.getLong();
            double x = buffer.getDouble();
            double y = buffer.getDouble();
            double z = buffer.getDouble();

            point3D = new Point3D(type, x, y, z, timestamp);
        }

        return point3D;
    }

    /**
     * Deserializes a single geo location from an array of bytes in Cyface binary format.
     *
     * @param bytes The bytes array to deserialize the geo locations from.
     * @return A poor mans list of objects (i.e. <code>Map</code>). Each map contains 5 entrys keyed with "timestamp",
     *         "lat", "lon", "speed" and "accuracy" with the appropriate values. The timestamp is a <code>long</code>,
     *         accuracy is an <code>int</code> and all other values are <code>double</code> values.
     */
    private LocationPoint deserializeGeoLocation(byte[] bytes) {
        LocationPoint locPoint = null;

        for (int i = 0; i < bytes.length; i += ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY) {
            ByteBuffer buffer = ByteBuffer
                    .wrap(Arrays.copyOfRange(bytes, i, i + ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY));

            // readout order ist important, its a byte buffer dude
            long timestamp = buffer.getLong();
            double latitude = buffer.getDouble();
            double longitude = buffer.getDouble();
            double speed = buffer.getDouble();
            int accuracy = buffer.getInt();

            locPoint = new LocationPoint(accuracy, longitude, latitude, speed, timestamp);

        }
        return locPoint;
    }

    public static class CyfaceCompressedDataProcessorException extends Exception {

        public CyfaceCompressedDataProcessorException(String message) {
            super(message);
        }

        /**
         * 
         */
        private static final long serialVersionUID = 3893090953252038103L;

    }

    @Override
    protected void finalize() throws Throwable {
        File tempFile = decompressedTempfile;
        if (tempFile.exists()) {
            tempFile.delete();
        }
        compressedBinaryInputStream.close();
        decompressedBinaryInputStream.close();
        decompressedBinaryOutputStream.close();
        inflaterInputStream.close();
    }

}
