package de.cyface.dataprocessor.impl;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipException;

import org.apache.commons.io.IOUtils;

import de.cyface.data.ByteSizes;
import de.cyface.data.LocationPoint;
import de.cyface.data.Point3D;
import de.cyface.data.Point3D.TypePoint3D;
import de.cyface.dataprocessor.AbstractCyfaceDataProcessor;
import de.cyface.dataprocessor.ICyfaceDataProcessor;

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

    static final int DEFAULT_INFLATER_BYTE_BUF = 4096;

    static final String TEMP_FOLDER = "uncompressed-temp/";

    // InputStream binaryInputStream;
    InputStream compressedBinaryInputStream;
    FileOutputStream uncompressedBinaryOutputStream;

    InflaterInputStream inflaterInputStream;

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
        Objects.requireNonNull(binaryInputStream, "InputStream must not be null.");
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
        uncompressed = !compressed;
        // this.binaryInputStream = binaryInputStream;
    }

    @Override
    public byte[] getUncompressedBinaryAsArray() throws CyfaceCompressedDataProcessorException, IOException {
        checkUncompressedOrThrowException();
        return Files.readAllBytes(uncompressedTempfile.toPath());
    }

    @Override
    public ICyfaceDataProcessor uncompress() throws CyfaceCompressedDataProcessorException, IOException {
        InputStream reader = null;
        if (!uncompressed) {
            boolean nowrap = false;
            boolean retry = true;

            while (retry && !uncompressed) {
                reader = new FileInputStream(compressedTempfile);
                this.compressedBinaryInputStream = new BufferedInputStream(reader);
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

            uncompressedBinaryInputStream = new BufferedInputStream(new FileInputStream(uncompressedTempfile));
        } else {
            reader = new FileInputStream(compressedTempfile);
            IOUtils.copy(reader, uncompressedBinaryOutputStream, 1024);
            uncompressedBinaryInputStream = new BufferedInputStream(new FileInputStream(uncompressedTempfile));
            if (reader != null) {
                reader.close();
            }
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

    @Override
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

    @Override
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

    @Override
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

    @Override
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
            // if given uncompressed, it can be null
            // closeStreamIfNotNull(binaryInputStream);
            closeStreamIfNotNull(compressedBinaryInputStream);
            closeStreamIfNotNull(inflaterInputStream);
            closeStreamIfNotNull(uncompressedBinaryInputStream);
            closeStreamIfNotNull(uncompressedBinaryOutputStream);
            closeStreamIfNotNull(tempLocStream);
            closeStreamIfNotNull(tempAccStream);
            closeStreamIfNotNull(tempRotStream);
            closeStreamIfNotNull(tempDirStream);
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

    private void closeStreamIfNotNull(Closeable closeable) throws IOException {
        if (closeable != null) {
            closeable.close();
        }
    }

    private void deleteFileIfNotNull(File file) throws IOException {
        if (file != null) {
            Files.delete(file.toPath());
        }
    }

}
