package de.cyface.dataprocessor.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

import de.cyface.dataprocessor.AbstractCyfaceDataProcessor;

public class CyfaceDataProcessorInMemoryImpl extends AbstractCyfaceDataProcessor {

    // separate temporary byte array parts for each sensor type
    byte[] compressedTempBin;
    byte[] tempLocBin;
    byte[] tempAccBin;
    byte[] tempRotBin;
    byte[] tempDirBin;

    byte[] uncompressedTempBin;

    public CyfaceDataProcessorInMemoryImpl(InputStream binaryInputStream, boolean compressed) {
        super(binaryInputStream, compressed);
        try {
            this.compressedTempBin = IOUtils.toByteArray(binaryInputStream);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // TODO Auto-generated constructor stub
    }

    @Override
    public byte[] getUncompressedBinaryAsArray() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub
        return uncompressedTempBin;
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    protected void prepare() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected InputStream getCompressedInputStream() {
        // TODO Auto-generated method stub
        return new ByteArrayInputStream(compressedTempBin);
    }

    @Override
    protected InputStream getUncompressedInputStream() {
        // TODO Auto-generated method stub
        return new ByteArrayInputStream(uncompressedTempBin);
    }

    @Override
    protected InputStream getSpecificLocInputStream() {
        // TODO Auto-generated method stub
        return new ByteArrayInputStream(tempLocBin);
    }

    @Override
    protected InputStream getSpecificAccInputStream() {
        // TODO Auto-generated method stub
        return new ByteArrayInputStream(tempAccBin);
    }

    @Override
    protected InputStream getSpecificRotInputStream() {
        // TODO Auto-generated method stub
        return new ByteArrayInputStream(tempRotBin);
    }

    @Override
    protected InputStream getSpecificDirInputStream() {
        // TODO Auto-generated method stub
        return new ByteArrayInputStream(tempDirBin);
    }

    @Override
    protected OutputStream getTempLocOutputStream() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected OutputStream getTempAccOutputStream() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected OutputStream getTempRotOutputStream() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected OutputStream getTempDirOutputStream() {
        // TODO Auto-generated method stub
        return null;
    }

}
