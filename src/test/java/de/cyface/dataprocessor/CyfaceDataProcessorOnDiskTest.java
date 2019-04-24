package de.cyface.dataprocessor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.cyface.data.LocationPoint;
import de.cyface.data.Point3D;
import de.cyface.dataprocessor.AbstractCyfaceDataProcessor.CyfaceCompressedDataProcessorException;
import de.cyface.dataprocessor.impl.CyfaceDataProcessorOnDiskImpl;

/**
 * 
 * @author Philipp Grubitzsch
 *
 */
public class CyfaceDataProcessorOnDiskTest {

    FileInputStream fileInputStream;
    CyfaceDataProcessorOnDiskImpl proc = null;

    @Before
    public void setUp() throws IOException {

    }

    @Test
    public void testUncompressCyfaceBinary() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/compressedCyfaceData").getFile());

        try {
            proc = new CyfaceDataProcessorOnDiskImpl(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        printOutHeaderInfoFromRawBinary(proc);

        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        assertEquals(individualBytes.length, 116398);

        ByteBuffer buffer = ByteBuffer.wrap(individualBytes);
        short formatVersion = buffer.order(ByteOrder.BIG_ENDIAN).getShort(0);
        assertThat(formatVersion, is(equalTo((short)1)));
        int numberOfGeoLocations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(2);
        assertThat(numberOfGeoLocations, is(equalTo(1711)));
        int numberOfAccelerations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(6);
        assertThat(numberOfAccelerations, is(equalTo(678)));
        int numberOfRotations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(10);
        assertThat(numberOfRotations, is(equalTo(1032)));
        int numberOfDirections = buffer.order(ByteOrder.BIG_ENDIAN).getInt(14);
        assertThat(numberOfDirections, is(equalTo(2)));

        // printOutData(proc);
        assertThat(proc.pollNextLocationPoint().toString(), is(equalTo(
                "timestamp=1521631263237,lon=13.728253648287687,lat=51.03168352640331,speed=0.18293093144893646,accuracy=1200")));
        assertThat(proc.pollNextAccelerationPoint().toString(), is(equalTo(
                "timestamp=1521631261383,x=-0.4956148862838745,y=3.8332340717315674,z=13.800600051879883,sensortype=ACC")));
        assertThat(proc.pollNextRotationPoint().toString(), is(equalTo(
                "timestamp=1521631263777,x=0.24593007564544678,y=-0.20202352106571198,z=0.5091384649276733,sensortype=ROT")));
        assertEquals(proc.pollNextDirectionPoint().toString(),
                "timestamp=1521632513534,x=-41.099998474121094,y=10.319999694824219,z=-7.619999885559082,sensortype=DIR");
    }

    @Test
    public void testUncompressCyfaceBinaryAndroid2018() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/android-format2018.ccyf").getFile());

        try {
            proc = new CyfaceDataProcessorOnDiskImpl(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        printOutHeaderInfoFromRawBinary(proc);

        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        assertEquals(35478, individualBytes.length);

        printOutData(proc);
    }

    @Test
    public void testUncompressCyfaceBinaryAndroid2019pseudoCorrupt()
            throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(
                this.getClass().getResource("/android-format2019-nowrap-only.ccyf").getFile());

        try {
            proc = new CyfaceDataProcessorOnDiskImpl(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        printOutHeaderInfoFromRawBinary(proc);

        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        assertEquals(22216054, individualBytes.length);

        printOutData(proc);
    }

    @Test
    public void testUncompressCyfaceBinaryAndroid2019() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(
                this.getClass().getResource("/android-format2019-nowrap-arbitrary.ccyf").getFile());

        try {
            proc = new CyfaceDataProcessorOnDiskImpl(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        printOutHeaderInfoFromRawBinary(proc);

        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        assertEquals(5638142, individualBytes.length);

        printOutData(proc);
    }

    @Test
    public void testDeserializeUncompressedCyfaceData() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/uncompressed.cyf").getFile());
        try {
            proc = new CyfaceDataProcessorOnDiskImpl(fileInputStream, false);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        printOutHeaderInfoFromRawBinary(proc);

        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        assertEquals(116398, individualBytes.length);

        ByteBuffer buffer = ByteBuffer.wrap(individualBytes);
        short formatVersion = buffer.order(ByteOrder.BIG_ENDIAN).getShort(0);
        assertThat(formatVersion, is(equalTo((short)1)));
        int numberOfGeoLocations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(2);
        assertThat(numberOfGeoLocations, is(equalTo(1711)));
        int numberOfAccelerations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(6);
        assertThat(numberOfAccelerations, is(equalTo(678)));
        int numberOfRotations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(10);
        assertThat(numberOfRotations, is(equalTo(1032)));
        int numberOfDirections = buffer.order(ByteOrder.BIG_ENDIAN).getInt(14);
        assertThat(numberOfDirections, is(equalTo(2)));

        // printOutData(proc);
        assertThat(proc.pollNextLocationPoint().toString(), is(equalTo(
                "timestamp=1521631263237,lon=13.728253648287687,lat=51.03168352640331,speed=0.18293093144893646,accuracy=1200")));
        assertThat(proc.pollNextAccelerationPoint().toString(), is(equalTo(
                "timestamp=1521631261383,x=-0.4956148862838745,y=3.8332340717315674,z=13.800600051879883,sensortype=ACC")));
        assertThat(proc.pollNextRotationPoint().toString(), is(equalTo(
                "timestamp=1521631263777,x=0.24593007564544678,y=-0.20202352106571198,z=0.5091384649276733,sensortype=ROT")));
        assertEquals(proc.pollNextDirectionPoint().toString(),
                "timestamp=1521632513534,x=-41.099998474121094,y=10.319999694824219,z=-7.619999885559082,sensortype=DIR");
    }

    /**
     * 
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    // @Ignore
    @Test
    public void testUncompressAndPrepareIOSData() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/iphone-working.ccyf").getFile());
        try {
            proc = new CyfaceDataProcessorOnDiskImpl(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        printOutHeaderInfoFromRawBinary(proc);

        // printOutData(proc);
        assertThat(proc.pollNextLocationPoint().toString(), is(equalTo(
                "timestamp=1531214224003,lon=13.72905942612675,lat=51.05205897246585,speed=0.0,accuracy=1000")));
    }

    /**
     * Early 2019 version of iOS component submitted uncompressed data
     * 
     * @throws CyfaceCompressedDataProcessorException
     * @throws IOException
     */
    @Test
    public void testUncompressedAndPrepareIOSData() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/ios-uncompressed-20190424.ccyf").getFile());
        try {
            proc = new CyfaceDataProcessorOnDiskImpl(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        printOutHeaderInfoFromRawBinary(proc);

        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        assertEquals(814182, individualBytes.length);

        printOutData(proc);
    }

    private void printOutHeaderInfoFromRawBinary(CyfaceDataProcessorOnDiskImpl proc)
            throws CyfaceCompressedDataProcessorException, IOException {
        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        System.out.println("uncompressed size: " + individualBytes.length);
        System.out.println("Cyface Format Version: " + proc.getHeader().getFormatVersion());
        System.out.println("geo: " + proc.getHeader().getNumberOfGeoLocations());
        System.out.println("acc: " + proc.getHeader().getNumberOfAccelerations());
        System.out.println("rot: " + proc.getHeader().getNumberOfRotations());
        System.out.println("dir: " + proc.getHeader().getNumberOfDirections());
    }

    @SuppressWarnings("unused")
    private void printOutData(CyfaceDataProcessor proc) throws CyfaceCompressedDataProcessorException, IOException {
        LocationPoint locItem;

        long start = System.nanoTime();
        int count = 0;
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

        long processNanoTime = (System.nanoTime() - start);
        System.out.println(
                processNanoTime / 1000000 + " ms - " + processNanoTime / count + " ns/item - " + count + " items");
    }

    @After
    public void tearDown() throws IOException {
        if (proc != null) {
            proc.close();
        }
    }
}
