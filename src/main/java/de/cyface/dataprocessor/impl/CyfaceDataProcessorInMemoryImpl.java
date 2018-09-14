package de.cyface.dataprocessor.impl;

import java.io.IOException;
import java.io.InputStream;

import de.cyface.data.LocationPoint;
import de.cyface.data.Point3D;
import de.cyface.dataprocessor.AbstractCyfaceDataProcessor;
import de.cyface.dataprocessor.ICyfaceDataProcessor;

public class CyfaceDataProcessorInMemoryImpl extends AbstractCyfaceDataProcessor {

    public CyfaceDataProcessorInMemoryImpl(InputStream binaryInputStream, boolean compressed) {
        super(binaryInputStream, compressed);
        // TODO Auto-generated constructor stub
    }

    @Override
    public byte[] getUncompressedBinaryAsArray() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ICyfaceDataProcessor uncompress() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LocationPoint pollNextLocationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Point3D pollNextAccelerationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Point3D pollNextRotationPoint() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Point3D pollNextDirectionPoint() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected void prepare() throws CyfaceCompressedDataProcessorException, IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected InputStream getCompressedInputStream() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected InputStream getUncompressedInputStream() {
        // TODO Auto-generated method stub
        return null;
    }

}
