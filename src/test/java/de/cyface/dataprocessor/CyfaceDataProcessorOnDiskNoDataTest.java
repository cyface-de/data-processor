package de.cyface.dataprocessor;

import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Test;

import de.cyface.dataprocessor.AbstractCyfaceDataProcessor.CyfaceCompressedDataProcessorException;
import de.cyface.dataprocessor.impl.CyfaceDataProcessorOnDiskImpl;

public class CyfaceDataProcessorOnDiskNoDataTest {

    CyfaceDataProcessorOnDiskImpl oocut;
    FileInputStream fileInputStream;

    @Test(expected = NullPointerException.class)
    public void testNullInput() throws IOException {
        oocut = new CyfaceDataProcessorOnDiskImpl(null, true);
        oocut = new CyfaceDataProcessorOnDiskImpl(null, false);
        oocut.close();
    }

    @Test
    public void testNoSensorData() throws IOException, CyfaceCompressedDataProcessorException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/nosensordata.ccyf").getFile());
        oocut = new CyfaceDataProcessorOnDiskImpl(fileInputStream, true);
        oocut.uncompress();
        printOutHeaderInfo(oocut);
        oocut.close();
    }

    private void printOutHeaderInfo(CyfaceDataProcessorOnDiskImpl proc)
            throws CyfaceCompressedDataProcessorException, IOException {
        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        System.out.println("uncompressed size: " + individualBytes.length);
        System.out.println("Cyface Format Version: " + proc.getHeader().getFormatVersion());
        System.out.println("geo: " + proc.getHeader().getNumberOfGeoLocations());
        System.out.println("acc: " + proc.getHeader().getNumberOfAccelerations());
        System.out.println("rot: " + proc.getHeader().getNumberOfRotations());
        System.out.println("dir: " + proc.getHeader().getNumberOfDirections());
    }

}
