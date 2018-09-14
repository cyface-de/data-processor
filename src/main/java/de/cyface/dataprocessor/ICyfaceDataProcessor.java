package de.cyface.dataprocessor;

import java.io.Closeable;
import java.io.IOException;

import de.cyface.data.LocationPoint;
import de.cyface.data.Point3D;
import de.cyface.dataprocessor.AbstractCyfaceDataProcessor.CyfaceCompressedDataProcessorException;

public interface ICyfaceDataProcessor extends Closeable {

    /**
     * 
     * @return True, if the data is already uncompressed.
     */
    public boolean isUncompressed();

    /**
     * 
     * @return True, if input is completely prepare for read out of sensor data. This include optional necessary
     *         uncompression, header readout and sensor data split.
     */
    public boolean isPrepared();

    /**
     * 
     * @return byte array for in memory processing. small binaries only!
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public byte[] getUncompressedBinaryAsArray() throws CyfaceCompressedDataProcessorException, IOException;

    /**
     * This method uncompress and prepare data to easily access arbitrary sensor data.
     * 
     * @return The instance of this specific Processor for fluently usage
     * @throws IOException
     * @throws CyfaceCompressedDataProcessorException
     */
    public ICyfaceDataProcessor uncompressAndPrepare() throws IOException, CyfaceCompressedDataProcessorException;

    /**
     * This method uncompress binary data.
     * 
     * @return The instance of this specific Processor for fluently usage
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public ICyfaceDataProcessor uncompress() throws CyfaceCompressedDataProcessorException, IOException;

    /**
     * Polls the next available geo location from binary temp file
     * 
     * @return an Location Point or null, if all entries have been already read.
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public LocationPoint pollNextLocationPoint() throws CyfaceCompressedDataProcessorException, IOException;

    /**
     * Polls the next available acceleration point from binary temp file
     * 
     * @return an acceleration point or null, if all entries have been already read.
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public Point3D pollNextAccelerationPoint() throws CyfaceCompressedDataProcessorException, IOException;

    /**
     * Polls the next available rotation point from binary temp file
     * 
     * @return an rotation point or null, if all entries have been already read.
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public Point3D pollNextRotationPoint() throws CyfaceCompressedDataProcessorException, IOException;

    /**
     * Polls the next available direction point from binary temp file
     * 
     * @return an direction point or null, if all entries have been already read.
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public Point3D pollNextDirectionPoint() throws CyfaceCompressedDataProcessorException, IOException;

    /**
     * Get the Header. Requires isUncompressed() true.
     * 
     * @return the CyfaceBinaryHeader
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    public CyfaceBinaryHeader getHeader() throws CyfaceCompressedDataProcessorException, IOException;

}
