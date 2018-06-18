package de.cyface.dataprocessor;

import java.io.FileInputStream;
import java.io.IOException;

import org.junit.Test;

import de.cyface.dataprocessor.CyfaceDataProcessor.CyfaceCompressedDataProcessorException;

public class CyfaceDataProcessorNoDataTest {

    CyfaceDataProcessor oocut;
    FileInputStream fileInputStream;

    @Test(expected = NullPointerException.class)
    public void testNullInput() throws IOException {
        oocut = new CyfaceDataProcessor(null, true);
        oocut = new CyfaceDataProcessor(null, false);
        oocut.close();
    }

    @Test
    public void testNoSensorData() throws IOException, CyfaceCompressedDataProcessorException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/nosensordata.ccyf").getFile());
        oocut = new CyfaceDataProcessor(fileInputStream, true);
        oocut.uncompress();
        printOutHeaderInfo(oocut);
        oocut.close();
    }

    private void printOutHeaderInfo(CyfaceDataProcessor proc)
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
