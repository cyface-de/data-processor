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
import java.nio.file.Paths;
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

public class CyfaceCompressedDataProcessor {

    static final int DEFAULT_INFLATER_BYTE_BUF = 1024;
    static final String UNCOMPRESS_FIRST_EXCEPTION = "Binary has to be uncompressed before other operations can be used.";
    static final String NOT_SO_MANY_GEOLOCATIONS_EXCEPTION = "The requested number of geolocations is higher than the available geolocations. Read number of available geolocations from Header first.";
    static final String TEMP_FOLDER = "uncompressed-temp/";

    InputStream compressedBinaryInputStream;
    FileOutputStream uncompressedBinaryOutputStream;
    BufferedInputStream uncompressedBinaryInputStream;
    InflaterInputStream inflaterInputStream;
    OutputStream binLocationTemp;
    OutputStream binAccTemp;
    OutputStream binRotTemp;
    OutputStream binDirTemp;

    String uncompressedTempfile;
    String locTempFile;
    String accTempFile;
    String rotTempFile;
    String dirTempFile;
    

    private boolean uncompressed;

    /**
     * 
     * @return True, if the data is already uncompressed.
     */
    public boolean isuncompressed() {
        return uncompressed;
    }

    private CyfaceBinaryHeader header;

    /**
     * Constructor for the Processor
     * 
     * @param compressedBinaryInputStream
     * @param uncompressedBinaryOutputStream
     * @throws IOException
     */
    public CyfaceCompressedDataProcessor(InputStream binaryInputStream, boolean compressed) throws IOException {
    	File tempFolder = new File(TEMP_FOLDER);
        if (!tempFolder.exists()) {
            tempFolder.mkdirs();
        }
        this.uncompressedTempfile = TEMP_FOLDER + UUID.randomUUID().toString();
        locTempFile = uncompressedTempfile + "_loc";
        accTempFile = uncompressedTempfile + "_acc";
        rotTempFile =  uncompressedTempfile + "_rot";
        dirTempFile = uncompressedTempfile + "_dir";
        
        this.uncompressedBinaryOutputStream = new FileOutputStream(uncompressedTempfile);
        if (!compressed) {
            uncompressed = true;
            IOUtils.copy(binaryInputStream, uncompressedBinaryOutputStream, 1024);
            uncompressedBinaryInputStream = new BufferedInputStream(new FileInputStream(uncompressedTempfile));
        } else {
            this.compressedBinaryInputStream = binaryInputStream;
        }
        
    }

    /**
     * This method uncompress and prepare data to easily access arbitrary sensor data.
     * 
     * @return The instance of this specific Processor for fluently usage
     * @throws IOException
     * @throws CyfaceCompressedDataProcessorException
     */
    public CyfaceCompressedDataProcessor uncompressAndPrepare()
            throws IOException, CyfaceCompressedDataProcessorException {
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
     * @return InputStream to read from for big binaries to be not completely processed in memory
     * @throws CyfaceCompressedDataProcessorException
     */
    public InputStream getUncompressedBinaryAsInputStream() throws CyfaceCompressedDataProcessorException {
        checkUncompressedOrThrowException();
        return uncompressedBinaryInputStream;
    }

    /**
     * 
     * @return byte array for in memory processing. small binaries only!
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public byte[] getUncompressedBinaryAsArray() throws CyfaceCompressedDataProcessorException, IOException {
        checkUncompressedOrThrowException();
        return Files.readAllBytes(Paths.get(uncompressedTempfile));
    }

    /**
     * 
     * @param locationsCount -1 to get all from offset. Reader header first to get the max number of retrievable geo locations.
     * @param locationsOffset skip given number of locations
     * @return
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public List<Map<String, ?>> getLocationDataAsList(int locationsOffset, int locationsCount)
            throws CyfaceCompressedDataProcessorException, IOException {
        checkUncompressedOrThrowException();
        checkHeaderRead();
        BufferedInputStream binLocationsIn = new BufferedInputStream(new FileInputStream(locTempFile));
        int bytesOffset = locationsOffset * ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY;
        int bytesTempOffset = bytesOffset;
        while(bytesTempOffset>0) {
        binLocationsIn.read();
        bytesTempOffset--;
        }
        
        int bytesToRead = locationsCount * ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY;
        byte[] locationBytes = null;
        if (locationsCount == -1) {
            int locationBytesCount = header.getBeginOfAccelerationsIndex() - header.getBeginOfGeoLocationsIndex()-bytesOffset;
            locationBytes = new byte[locationBytesCount];
            binLocationsIn.read(locationBytes, 0, locationBytesCount);
        } else {
            if (locationsCount > header.getNumberOfGeoLocations()) {
            	binLocationsIn.close();
                throw new CyfaceCompressedDataProcessorException(NOT_SO_MANY_GEOLOCATIONS_EXCEPTION);
            } else {
                System.out.println("#" + (bytesToRead));
                locationBytes = new byte[bytesToRead];
                binLocationsIn.read(locationBytes, 0, bytesToRead);
            }
        }

        List<Map<String, ?>> geoLocations = deserializeGeoLocations(locationBytes);
        binLocationsIn.close();
        return geoLocations;
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
        this.binLocationTemp = new FileOutputStream(locTempFile);
        int locationBytesCount = header.getBeginOfAccelerationsIndex() - header.getBeginOfGeoLocationsIndex();
        System.out.println(locationBytesCount);
        copyStream(uncompressedBinaryInputStream, binLocationTemp, 0, locationBytesCount);
        // write out accelerometer data
        this.binAccTemp = new FileOutputStream(accTempFile);
        int accBytesCount = header.getBeginOfRotationsIndex() - header.getBeginOfAccelerationsIndex();
        System.out.println(accBytesCount);
        copyStream(uncompressedBinaryInputStream, binAccTemp, 0, accBytesCount);
        // write out rotation data
        this.binRotTemp = new FileOutputStream(rotTempFile);
        int rotBytesCount = header.getBeginOfDirectionsIndex() - header.getBeginOfRotationsIndex();
        System.out.println(rotBytesCount);
        copyStream(uncompressedBinaryInputStream, binRotTemp, 0, rotBytesCount);
        // write out direction data
        this.binDirTemp = new FileOutputStream(dirTempFile);
        int dirBytesCount = ByteSizes.BYTES_IN_ONE_POINT_ENTRY * header.getNumberOfDirections();
        System.out.println(dirBytesCount);
        copyStream(uncompressedBinaryInputStream, binDirTemp, 0, dirBytesCount);
    }

    /**
     * Copies a part of an input stream to an output stream
     * 
     * @param input the stream to read from
     * @param output the stream to write to
     * @param offset start point of the part to read from the input
     * @param readNumberOfBytes total number of bytes to copy from offset
     * @throws IOException
     */
    private static void copyStream(InputStream input, OutputStream output, long offset, long readNumberOfBytes) throws IOException {
    	int bufSize = 1024;
    	if(readNumberOfBytes < bufSize) {
    		bufSize = 1;
    	}
        for (int i = 0; i < offset; i++)
            input.read(); // skip unwanted bytes
        byte[] buf = new byte[1024];
        int bytesCount;
        int bytesRead = 0;
        // System.out.println("Read bytes: " + eof);
        while ((bytesCount = input.read(buf, 0, bufSize)) != -1 && bytesRead < readNumberOfBytes) {
        	bytesRead += bytesCount;
        	output.write(buf, 0, bytesCount);
        	if((readNumberOfBytes - bytesRead)<bufSize) {
        		bufSize = Math.toIntExact(readNumberOfBytes - bytesRead);
        	}
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
    

}
