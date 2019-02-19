package de.cyface.dataprocessor.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
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

import de.cyface.dataprocessor.AbstractCyfaceDataProcessor;

/**
 * This implementation of the CyfaceDataProcessor is optimized to use as less memory as possible. Therefore the
 * local file system is utilized to create binary temp files for each sensor.
 * 
 * @author Philipp Grubitzsch
 * @since 0.2.0
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

    FileInputStream accFileInputStream;

    @Override
    protected InputStream getSpecificAccInputStream() {
        // TODO Auto-generated method stub
        if (accFileInputStream != null) {
            return accFileInputStream;
        } else {
            if (tempAccFile != null) {
                try {
                    accFileInputStream = new FileInputStream(tempAccFile);
                    return accFileInputStream;
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return null;
                }
            } else {
                return new ByteArrayInputStream(new byte[0]);
            }
        }
    }

    FileInputStream locFileInputStream;

    @Override
    protected InputStream getSpecificLocInputStream() {
        if (locFileInputStream != null) {
            return locFileInputStream;
        } else {
            if (tempLocFile != null) {
                try {
                    locFileInputStream = new FileInputStream(tempLocFile);
                    return locFileInputStream;
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return null;
                }
            } else {
                return new ByteArrayInputStream(new byte[0]);
            }
        }
    }

    FileInputStream rotFileInputStream;

    @Override
    protected InputStream getSpecificRotInputStream() {
        if (rotFileInputStream != null) {
            return rotFileInputStream;
        } else {
            if (tempRotFile != null) {
                try {
                    rotFileInputStream = new FileInputStream(tempRotFile);
                    return rotFileInputStream;
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return null;
                }
            } else {
                return new ByteArrayInputStream(new byte[0]);
            }
        }
    }

    FileInputStream dirFileInputStream;

    @Override
    protected InputStream getSpecificDirInputStream() {
        if (dirFileInputStream != null) {
            return dirFileInputStream;
        } else {
            if (tempDirFile != null) {
                try {
                    dirFileInputStream = new FileInputStream(tempDirFile);
                    return dirFileInputStream;
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return null;
                }
            } else {
                return new ByteArrayInputStream(new byte[0]);
            }
        }
    }

    @Override
    protected OutputStream getTempLocOutputStream() {
        tempLocFile = new File(uncompressedTempfile + "_loc");
        try {
            return new FileOutputStream(tempLocFile);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected OutputStream getTempAccOutputStream() {
        tempAccFile = new File(uncompressedTempfile + "_acc");
        try {
            return new FileOutputStream(tempAccFile);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected OutputStream getTempRotOutputStream() {
        tempRotFile = new File(uncompressedTempfile + "_rot");
        try {
            return new FileOutputStream(tempRotFile);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected OutputStream getTempDirOutputStream() {
        tempDirFile = new File(uncompressedTempfile + "_dir");
        try {
            return new FileOutputStream(tempDirFile);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }

}
