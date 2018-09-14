package de.cyface.dataprocessor.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

import de.cyface.data.ByteSizes;
import de.cyface.data.LocationPoint;
import de.cyface.data.Point3D;
import de.cyface.dataprocessor.AbstractCyfaceDataProcessor;

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
public class CyfaceDataProcessorOnDiskImpl extends AbstractCyfaceDataProcessor {

    static final String TEMP_FOLDER = "uncompressed-temp/";

    // separate temporary file parts for each sensor type
    File compressedTempfile;
    File tempLocFile;
    File tempAccFile;
    File tempRotFile;
    File tempDirFile;

    File uncompressedTempfile;

    /**
     * Constructor for the Processor
     * 
     * @param binaryInputStream the binary input either compressed or uncompressed
     * @param compressed flag to tell the processor if the binary input is compressed
     * @throws IOException
     */
    public CyfaceDataProcessorOnDiskImpl(InputStream binaryInputStream, boolean compressed) throws IOException {
        super(binaryInputStream, compressed);

        File tempFolder = new File(TEMP_FOLDER);
        if (!tempFolder.exists()) {
            tempFolder.mkdirs();
        }
        this.uncompressedTempfile = new File(TEMP_FOLDER + UUID.randomUUID().toString());
        this.compressedTempfile = new File(TEMP_FOLDER + UUID.randomUUID().toString() + "_compressed");
        OutputStream compressedTempFileOutputStream = new FileOutputStream(compressedTempfile);
        IOUtils.copy(binaryInputStream, compressedTempFileOutputStream);
        binaryInputStream.close();
        compressedTempFileOutputStream.flush();
        compressedTempFileOutputStream.close();
        this.uncompressedBinaryOutputStream = new FileOutputStream(uncompressedTempfile);
    }

    @Override
    public byte[] getUncompressedBinaryAsArray() throws CyfaceCompressedDataProcessorException, IOException {
        checkUncompressedOrThrowException();
        return Files.readAllBytes(uncompressedTempfile.toPath());
    }

    @Override
    protected void prepare() throws CyfaceCompressedDataProcessorException, IOException {
        // read the header first.
        // getHeader();

        // write out geo locations
        tempLocFile = new File(uncompressedTempfile + "_loc");
        FileOutputStream binLocationTemp = new FileOutputStream(tempLocFile);
        int locationBytesCount = this.getHeader().getNumberOfGeoLocations() * ByteSizes.BYTES_IN_ONE_GEO_LOCATION_ENTRY;
        copyStream(uncompressedBinaryInputStream, binLocationTemp, 0, locationBytesCount);
        binLocationTemp.close();

        // write out accelerometer data
        if (this.getHeader().getNumberOfAccelerations() > 0) {

            tempAccFile = new File(uncompressedTempfile + "_acc");
            FileOutputStream binAccTemp = new FileOutputStream(tempAccFile);
            int accBytesCount = this.getHeader().getNumberOfAccelerations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY;
            copyStream(uncompressedBinaryInputStream, binAccTemp, 0, accBytesCount);
            binAccTemp.close();
        }

        // write out rotation data
        if (this.getHeader().getNumberOfRotations() > 0) {
            tempRotFile = new File(uncompressedTempfile + "_rot");
            FileOutputStream binRotTemp = new FileOutputStream(tempRotFile);
            int rotBytesCount = this.getHeader().getNumberOfRotations() * ByteSizes.BYTES_IN_ONE_POINT_ENTRY;
            copyStream(uncompressedBinaryInputStream, binRotTemp, 0, rotBytesCount);
            binRotTemp.close();
        }

        // write out direction data
        if (this.getHeader().getNumberOfDirections() > 0) {
            tempDirFile = new File(uncompressedTempfile + "_dir");
            FileOutputStream binDirTemp = new FileOutputStream(tempDirFile);
            int dirBytesCount = ByteSizes.BYTES_IN_ONE_POINT_ENTRY * this.getHeader().getNumberOfDirections();
            copyStream(uncompressedBinaryInputStream, binDirTemp, 0, dirBytesCount);
            binDirTemp.close();
        }

        // close input stream
        uncompressedBinaryInputStream.close();

        prepared = true;
    }

    @Override
    public void close() {
        try {
            super.close();
            // if given uncompressed, it can be null
            // closeStreamIfNotNull(binaryInputStream);

        } catch (IOException e) {
            throw new RuntimeException("Could not close Stream, while trying to close DataProcessor.", e);
        }

        try {
            deleteFileIfNotNull(uncompressedTempfile);
            deleteFileIfNotNull(compressedTempfile);
            deleteFileIfNotNull(tempLocFile);
            deleteFileIfNotNull(tempAccFile);
            deleteFileIfNotNull(tempRotFile);
            deleteFileIfNotNull(tempDirFile);
        } catch (IOException e) {
            throw new RuntimeException("Could not delete all tempfiles: " + e.getMessage());
        }

    }

    private void deleteFileIfNotNull(File file) throws IOException {
        if (file != null) {
            Files.delete(file.toPath());
        }
    }

    @Override
    protected InputStream getCompressedInputStream() {
        try {
            return new FileInputStream(compressedTempfile);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected InputStream getUncompressedInputStream() {
        try {
            return new BufferedInputStream(new FileInputStream(uncompressedTempfile));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected InputStream getSpecificAccInputStream() {
        // TODO Auto-generated method stub
        try {
            return new FileInputStream(tempAccFile);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected InputStream getSpecificLocInputStream() {
        // TODO Auto-generated method stub
        try {
            return new FileInputStream(tempLocFile);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected InputStream getSpecificRotInputStream() {
        // TODO Auto-generated method stub
        try {
            return new FileInputStream(tempRotFile);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected InputStream getSpecificDirInputStream() {
        // TODO Auto-generated method stub
        try {
            return new FileInputStream(tempDirFile);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

}
