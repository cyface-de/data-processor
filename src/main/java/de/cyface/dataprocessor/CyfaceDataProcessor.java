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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.IOUtils;

import de.cyface.synchronization.ByteSizes;

/**
 * The CyfaceDataProcessor can be used to easily get Strings of human readable sensor data from the Cyface binary format
 * plain (.cyf) or compressed (.ccyf). It is optimized to use as less memory as possible. Therefore it uses the local
 * file system to create binary temp files for each sensor. Several functions allow to poll single sensor values from
 * those files as Map<String,?> Objects, which contain the human readable data for further processing.
 * 
 * @author Philipp Grubitzsch
 *
 */
public class CyfaceDataProcessor {

    static final int DEFAULT_INFLATER_BYTE_BUF = 1024;
    static final String UNCOMPRESS_FIRST_EXCEPTION = "Binary has to be uncompressed before other operations can be used.";
    static final String PREPARE_FIRST_EXCEPTION = "Binary has to be prepared before this operations can be used.";
    static final String NOT_SO_MANY_GEOLOCATIONS_EXCEPTION = "The requested number of geolocations is higher than the available geolocations. Read number of available geolocations from Header first.";

    static final String TEMP_FOLDER = "uncompressed-temp/";

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
        this.uncompressedTempfile = new File(TEMP_FOLDER + UUID.randomUUID().toString());
        this.uncompressedBinaryOutputStream = new FileOutputStream(uncompressedTempfile);
        if (!compressed) {
            uncompressed = true;
            IOUtils.copy(binaryInputStream, uncompressedBinaryOutputStream, 1024);
            uncompressedBinaryInputStream = new BufferedInputStream(new FileInputStream(uncompressedTempfile));
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
        if (!uncompressed) {
            Inflater decompressor = new Inflater();
            this.inflaterInputStream = new InflaterInputStream(compressedBinaryInputStream, decompressor,
                    DEFAULT_INFLATER_BYTE_BUF);

            // byte[] buf = new byte[DEFAULT_INFLATER_BYTE_BUF];
            // int bytesCount;
            //
            // // decompress in blocks of the set inflater buffer size and write to output
            // while ((bytesCount = inflaterInputStream.read(buf, 0, DEFAULT_INFLATER_BYTE_BUF)) != -1) {
            // decompressedBinaryOutputStream.write(buf, 0, bytesCount);
            // }
            IOUtils.copy(inflaterInputStream, uncompressedBinaryOutputStream, 1024);

            // close streams after write out is done
            uncompressedBinaryOutputStream.close();
            compressedBinaryInputStream.close();
            inflaterInputStream.close();
            uncompressed = true;
            uncompressedBinaryInputStream = new BufferedInputStream(new FileInputStream(uncompressedTempfile));
        }
        prepare();
        return this;
    }

    /**
     * 
     * @param pointCount 0 to get all. Reader header first to get the max number of retrievable geo locations.
     * @return
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    @Deprecated
    public List<Map<String, ?>> getLocationDataAsList(int pointCount)
            throws CyfaceCompressedDataProcessorException, IOException {
        checkUncompressedOrThrowException();
        checkHeaderRead();
        BufferedInputStream tempLocStream = new BufferedInputStream(new FileInputStream(tempLocFile), 4096);
        byte[] locationBytes = null;

        // TODO this is not memory save in case of huge files
        if (pointCount == 0) {
            int locationBytesCount = header.getBeginOfAccelerationsIndex() - header.getBeginOfGeoLocationsIndex();
            locationBytes = new byte[locationBytesCount];
            int read = tempLocStream.read(locationBytes, 0, locationBytesCount);
        } else {

            if (pointCount > header.getNumberOfGeoLocations()) {
                throw new CyfaceCompressedDataProcessorException(NOT_SO_MANY_GEOLOCATIONS_EXCEPTION);
            } else {
                int bytesEnd = header.getBeginOfGeoLocationsIndex()
                        + pointCount * ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY;
                System.out.println("#" + (bytesEnd - header.getBeginOfGeoLocationsIndex()));
                locationBytes = new byte[bytesEnd - header.getBeginOfGeoLocationsIndex()];
                tempLocStream.read(locationBytes, 0, bytesEnd);
            }
        }

        List<Map<String, ?>> geoLocations = deserializeGeoLocations(locationBytes);
        return geoLocations;
    }

    BufferedInputStream tempLocStream;

    /**
     * Polls the next available geo location from binary temp file
     * 
     * @return an Location Point or null, if all entries have been already read.
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public Map<String, ?> pollNextLocationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
        if (tempLocStream == null) {
            tempLocStream = new BufferedInputStream(new FileInputStream(tempLocFile),
                    ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY);
        }
        byte[] locationBytes = new byte[ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY];
        int read = tempLocStream.read(locationBytes, 0, ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY);
        if (read != -1) {
            return deserializeGeoLocations(locationBytes).get(0);
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
    public Map<String, ?> pollNextAccelerationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
        if (tempAccStream == null) {
            tempAccStream = new BufferedInputStream(new FileInputStream(tempAccFile),
                    ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        }

        Map<String, ?> nextPoint = pollNext3DPoint(tempAccStream);
        if (nextPoint == null) {
            tempAccStream.close();
        }
        return nextPoint;
    }

    BufferedInputStream tempRotStream;

    public Map<String, ?> pollNextRotationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
        if (tempRotStream == null) {
            tempRotStream = new BufferedInputStream(new FileInputStream(tempRotFile),
                    ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        }

        Map<String, ?> nextPoint = pollNext3DPoint(tempRotStream);
        if (nextPoint == null) {
            tempRotStream.close();
        }
        return nextPoint;
    }

    BufferedInputStream tempDirStream;

    public Map<String, ?> pollNextDirectionPoint() throws CyfaceCompressedDataProcessorException, IOException {
        checkPreparedOrThrowException();
        if (tempDirStream == null) {
            tempDirStream = new BufferedInputStream(new FileInputStream(tempDirFile),
                    ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        }

        Map<String, ?> nextPoint = pollNext3DPoint(tempDirStream);
        if (nextPoint == null) {
            tempDirStream.close();
        }
        return nextPoint;
    }

    private Map<String, ?> pollNext3DPoint(BufferedInputStream bufInputStream) throws IOException {
        byte[] point3DBytes = new byte[ByteSizes.BYTES_IN_ONE_POINT_ENTRY];
        int read = bufInputStream.read(point3DBytes, 0, ByteSizes.BYTES_IN_ONE_POINT_ENTRY);
        if (read != -1) {
            return deserializePoints3D(point3DBytes).get(0);
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
        this.readHeader();

        // write out geo locations
        tempLocFile = new File(uncompressedTempfile + "_loc");
        FileOutputStream binLocationTemp = new FileOutputStream(tempLocFile);
        int locationBytesCount = header.getNumberOfGeoLocations() * ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY;
        System.out.println(locationBytesCount);
        copyStream(uncompressedBinaryInputStream, binLocationTemp, 0, locationBytesCount);
        binLocationTemp.close();

        // write out accelerometer data
        tempAccFile = new File(uncompressedTempfile + "_acc");
        FileOutputStream binAccTemp = new FileOutputStream(tempAccFile);
        int accBytesCount = header.getNumberOfAccelerations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY;
        copyStream(uncompressedBinaryInputStream, binAccTemp, 0, accBytesCount);
        binAccTemp.close();

        // write out rotation data
        tempRotFile = new File(uncompressedTempfile + "_rot");
        FileOutputStream binRotTemp = new FileOutputStream(tempRotFile);
        int rotBytesCount = header.getNumberOfRotations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY;
        copyStream(uncompressedBinaryInputStream, binRotTemp, 0, rotBytesCount);
        binRotTemp.close();

        // write out direction data
        tempDirFile = new File(uncompressedTempfile + "_dir");
        FileOutputStream binDirTemp = new FileOutputStream(tempDirFile);
        int dirBytesCount = ByteSizes.BYTES_IN_ONE_POINT_ENTRY * header.getNumberOfDirections();
        copyStream(uncompressedBinaryInputStream, binRotTemp, 0, dirBytesCount);
        binDirTemp.close();

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
            System.out.println(totalRead);

        }
    }

    public CyfaceBinaryHeader getHeader() throws CyfaceCompressedDataProcessorException, IOException {
        if (header == null) {
            readHeader();
        }

        return header;
    }

    private void checkUncompressedOrThrowException() throws CyfaceCompressedDataProcessorException {
        if (!uncompressed) {
            throw new CyfaceCompressedDataProcessorException(UNCOMPRESS_FIRST_EXCEPTION);
        }
    }

    private void checkPreparedOrThrowException() throws CyfaceCompressedDataProcessorException {
        if (!prepared) {
            throw new CyfaceCompressedDataProcessorException(UNCOMPRESS_FIRST_EXCEPTION);
        }
    }

    private void checkHeaderRead() throws CyfaceCompressedDataProcessorException, IOException {
        if (header == null) {
            readHeader();
        }
    }

    /**
     * Deserializes a list of 3D sample points (i.e. acceleration, rotation or direction) from an array of bytes in
     * Cyface binary format.
     *
     * @param bytes The bytes array to deserialize the sample points from.
     * @return A poor mans list of objects (i.e. <code>Map</code>). Each map contains 4 entrys for x, y, z and timestamp
     *         with the corresponding values. A timestamp is a <code>long</code>, all other values are
     *         <code>double</code>.
     */
    private List<Map<String, ?>> deserializePoints3D(byte[] bytes) {
        List<Map<String, ?>> ret = new ArrayList<>();

        for (int i = 0; i < bytes.length; i += ByteSizes.BYTES_IN_ONE_POINT_ENTRY) {
            ByteBuffer buffer = ByteBuffer.wrap(Arrays.copyOfRange(bytes, i, i + ByteSizes.BYTES_IN_ONE_POINT_ENTRY));
            Map<String, Object> entry = new HashMap<>(4);
            entry.put("timestamp", buffer.getLong());
            entry.put("x", buffer.getDouble());
            entry.put("y", buffer.getDouble());
            entry.put("z", buffer.getDouble());

            ret.add(entry);
        }

        return ret;
    }

    /**
     * Deserializes a list of geo locations from an array of bytes in Cyface binary format.
     *
     * @param bytes The bytes array to deserialize the geo locations from.
     * @return A poor mans list of objects (i.e. <code>Map</code>). Each map contains 5 entrys keyed with "timestamp",
     *         "lat", "lon", "speed" and "accuracy" with the appropriate values. The timestamp is a <code>long</code>,
     *         accuracy is an <code>int</code> and all other values are <code>double</code> values.
     */
    private List<Map<String, ?>> deserializeGeoLocations(byte[] bytes) {
        List<Map<String, ?>> ret = new ArrayList<>();
        for (int i = 0; i < bytes.length; i += ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY) {
            ByteBuffer buffer = ByteBuffer
                    .wrap(Arrays.copyOfRange(bytes, i, i + ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY));
            Map<String, Object> entry = new HashMap<>(5);
            entry.put("timestamp", buffer.getLong());
            entry.put("lat", buffer.getDouble());
            entry.put("lon", buffer.getDouble());
            entry.put("speed", buffer.getDouble());
            entry.put("accuracy", buffer.getInt());

            ret.add(entry);
        }
        return ret;
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
        File tempFile = uncompressedTempfile;
        if (tempFile.exists()) {
            tempFile.delete();
        }
        compressedBinaryInputStream.close();
        uncompressedBinaryInputStream.close();
        uncompressedBinaryOutputStream.close();
        inflaterInputStream.close();
    }

}
