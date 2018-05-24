package de.cyface.dataprocessor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import de.cyface.dataprocessor.CyfaceDataProcessor.CyfaceCompressedDataProcessorException;

public class DecompressorTest {

    FileInputStream fileInputStream;

    @Before
    public void setUp() throws IOException {

    }

    @Test
    public void testDecompressCyfaceBinary() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/compressedCyfaceData").getFile());
        CyfaceDataProcessor proc = null;
        try {
            proc = new CyfaceDataProcessor(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        assertEquals(individualBytes.length, 116398);

        System.out.println(proc.getHeader().getNumberOfGeoLocations());

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

        List<Map<String, ?>> geoLocs = proc.getLocationDataAsList(0);
        geoLocs.forEach(item -> {
            System.out.println(item.toString());
        });
    }

    @Test
    public void testDeserializeHugeUncompressedCyfaceData() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/uncompressedCyfaceBigData.cyf").getFile());
        CyfaceDataProcessor proc = null;
        try {
            proc = new CyfaceDataProcessor(fileInputStream, false);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        assertEquals(individualBytes.length, 118255978);

        System.out.println(proc.getHeader().getNumberOfGeoLocations());

        ByteBuffer buffer = ByteBuffer.wrap(individualBytes);
        short formatVersion = buffer.order(ByteOrder.BIG_ENDIAN).getShort(0);
        assertThat(formatVersion, is(equalTo((short)1)));
        int numberOfGeoLocations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(2);
        assertThat(numberOfGeoLocations, is(equalTo(66254)));
        int numberOfAccelerations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(6);
        assertThat(numberOfAccelerations, is(equalTo(1810475)));
        int numberOfRotations = buffer.order(ByteOrder.BIG_ENDIAN).getInt(10);
        assertThat(numberOfRotations, is(equalTo(1810488)));
        int numberOfDirections = buffer.order(ByteOrder.BIG_ENDIAN).getInt(14);
        assertThat(numberOfDirections, is(equalTo(0)));

        // List<Map<String, ?>> geoLocs = proc.getLocationDataAsList(0);
        // geoLocs.forEach(item -> {
        // System.out.println(item.toString());
        // });

        Map<String, ?> locItem;
        while ((locItem = proc.pollNextLocationPoint()) != null) {
            System.out.println(locItem.toString());
        }

        Map<String, ?> accItem;
        while ((accItem = proc.pollNextAccelerationPoint()) != null) {
            System.out.println(accItem.toString());
        }

        Map<String, ?> rotItem;
        while ((rotItem = proc.pollNextRotationPoint()) != null) {
            System.out.println(rotItem.toString());
        }

        Map<String, ?> dirItem;
        while ((dirItem = proc.pollNextDirectionPoint()) != null) {
            System.out.println(dirItem.toString());
        }

    }

    @Test
    public void bigDataTestDDLE() throws CyfaceCompressedDataProcessorException, IOException {
        fileInputStream = new FileInputStream(this.getClass().getResource("/cyfaceDDLE.ccyf").getFile());
        CyfaceDataProcessor proc = null;
        try {
            proc = new CyfaceDataProcessor(fileInputStream, true);
            proc.uncompressAndPrepare();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        byte[] individualBytes = proc.getUncompressedBinaryAsArray();
        System.out.println("uncompressed size: " + individualBytes.length);
        System.out.println("geo: " + proc.getHeader().getNumberOfGeoLocations());
        System.out.println("acc: " + proc.getHeader().getNumberOfAccelerations());
        System.out.println("rot: " + proc.getHeader().getNumberOfRotations());
        System.out.println("dir: " + proc.getHeader().getNumberOfDirections());

        // List<Map<String, ?>> geoLocs = proc.getLocationDataAsList(100);
        // geoLocs.forEach(item -> {
        // System.out.println(item.toString());
        // });
    }

    private String convertTime(long time) {
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);
    }
}
