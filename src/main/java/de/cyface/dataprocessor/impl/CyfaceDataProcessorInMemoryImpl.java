package de.cyface.dataprocessor.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

import de.cyface.dataprocessor.AbstractCyfaceDataProcessor;

/**
 * This implementation of the CyfaceDataProcessor is optimized for maximum performance. Therefore memory (RAM) is
 * utilized to create binary temp arrays for each sensor.
 * 
 * @author Philipp Grubitzsch
 * @since 0.2.0
 *
 */
public class CyfaceDataProcessorInMemoryImpl extends AbstractCyfaceDataProcessor {

    // separate temporary byte array parts for each sensor type
    byte[] compressedTempBin;
    ByteArrayOutputStream tempLocBin;
    ByteArrayOutputStream tempAccBin;
    ByteArrayOutputStream tempRotBin;
    ByteArrayOutputStream tempDirBin;

    ByteArrayOutputStream uncompressedTempBin;

    public CyfaceDataProcessorInMemoryImpl(InputStream binaryInputStream, boolean compressed) {
        super(binaryInputStream, compressed);
        try {
            this.compressedTempBin = IOUtils.toByteArray(binaryInputStream);
            this.uncompressedTempBin = new ByteArrayOutputStream();
            this.uncompressedBinaryOutputStream = uncompressedTempBin;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // TODO Auto-generated constructor stub
    }

    @Override
    public byte[] getUncompressedBinaryAsArray() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub
        return uncompressedTempBin.toByteArray();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    protected InputStream getCompressedInputStream() {
        return new ByteArrayInputStream(compressedTempBin);
    }

    @Override
    protected InputStream getUncompressedInputStream() {
        return new ByteArrayInputStream(uncompressedTempBin.toByteArray());
    }

    @Override
    protected InputStream getSpecificLocInputStream() {
        if (tempLocBin != null) {
            return new ByteArrayInputStream(tempLocBin.toByteArray());
        } else {
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    @Override
    protected InputStream getSpecificAccInputStream() {
        if (tempAccBin != null) {
            return new ByteArrayInputStream(tempAccBin.toByteArray());
        } else {
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    @Override
    protected InputStream getSpecificRotInputStream() {
        if (tempRotBin != null) {
            return new ByteArrayInputStream(tempRotBin.toByteArray());
        } else {
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    @Override
    protected InputStream getSpecificDirInputStream() {
        if (tempDirBin != null) {
            return new ByteArrayInputStream(tempDirBin.toByteArray());
        } else {
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    @Override
    protected OutputStream getTempLocOutputStream() {
        this.tempLocBin = new ByteArrayOutputStream();
        return tempLocBin;
    }

    @Override
    protected OutputStream getTempAccOutputStream() {
        this.tempAccBin = new ByteArrayOutputStream();
        return tempAccBin;
    }

    @Override
    protected OutputStream getTempRotOutputStream() {
        this.tempRotBin = new ByteArrayOutputStream();
        return tempRotBin;
    }

    @Override
    protected OutputStream getTempDirOutputStream() {
        this.tempDirBin = new ByteArrayOutputStream();
        return tempDirBin;
    }

}
