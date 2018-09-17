package de.cyface.dataprocessor.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

import de.cyface.dataprocessor.AbstractCyfaceDataProcessor;

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
        return new ByteArrayInputStream(tempLocBin.toByteArray());
    }

    @Override
    protected InputStream getSpecificAccInputStream() {
        // TODO Auto-generated method stub
        return new ByteArrayInputStream(tempAccBin.toByteArray());
    }

    @Override
    protected InputStream getSpecificRotInputStream() {
        return new ByteArrayInputStream(tempRotBin.toByteArray());
    }

    @Override
    protected InputStream getSpecificDirInputStream() {
        return new ByteArrayInputStream(tempDirBin.toByteArray());
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
