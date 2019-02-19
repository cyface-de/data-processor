package de.cyface.dataprocessor;

import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Test;

import de.cyface.data.LocationPoint;
import de.cyface.data.Point3D;
import de.cyface.dataprocessor.AbstractCyfaceDataProcessor.CyfaceCompressedDataProcessorException;
import de.cyface.dataprocessor.impl.CyfaceDataProcessorInMemoryImpl;
import de.cyface.dataprocessor.impl.CyfaceDataProcessorOnDiskImpl;

public class CyfaceDataProcessorPerformanceTest {

    FileInputStream fileInputStream;
    CyfaceDataProcessor proc = null;
    long start;
    int count = 0;

    @Test
    public void testInmemoryPerformance() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/full-sensor-example.ccyf").getFile());

        try {
            proc = new CyfaceDataProcessorInMemoryImpl(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        start = System.nanoTime();
        printOutData(proc);
        long processNanoTime = (System.nanoTime() - start);

        assertTrue((processNanoTime / 1000000) < 500); // inmemory processing should be easily done in 500 ms
        System.out.println(
                processNanoTime / 1000000 + " ms - " + processNanoTime / count + " ns/item - " + count + " items");
    }

    @Test
    public void testOnDiskPerformance() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/full-sensor-example.ccyf").getFile());

        try {
            proc = new CyfaceDataProcessorOnDiskImpl(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        start = System.nanoTime();
        printOutData(proc);
        long processNanoTime = (System.nanoTime() - start);

        assertTrue((processNanoTime / 1000000) < 5000); // on disk processing should be easily done in 500 ms
        System.out.println(
                processNanoTime / 1000000 + " ms - " + processNanoTime / count + " ns/item - " + count + " items");

    }

    @SuppressWarnings("unused")
    private void printOutData(CyfaceDataProcessor proc) throws CyfaceCompressedDataProcessorException, IOException {
        LocationPoint locItem;

        while ((locItem = proc.pollNextLocationPoint()) != null) {
            count++;
            // System.out.println(locItem.toString());
        }

        Point3D accItem;
        while ((accItem = proc.pollNextAccelerationPoint()) != null) {
            count++;
            // System.out.println(accItem.toString());
        }

        Point3D rotItem;
        while ((rotItem = proc.pollNextRotationPoint()) != null) {
            count++;
            // System.out.println(rotItem.toString());
        }

        Point3D dirItem;
        while ((dirItem = proc.pollNextDirectionPoint()) != null) {
            count++;
            // System.out.println(dirItem.toString());
        }
    }

    @After
    public void tearDown() throws IOException {
        if (proc != null) {
            proc.close();
        }
    }
}
