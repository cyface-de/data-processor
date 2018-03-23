package de.cyface.dataprocessor;

public class CyfaceBinaryHeader {
    private short formatVersion;
    private int numberOfGeoLocations;
    private int numberOfAccelerations;
    private int numberOfRotations;
    private int numberOfDirections;
    private int beginOfGeoLocationsIndex;
    private int beginOfAccelerationsIndex;
    private int beginOfRotationsIndex;

    public short getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(short formatVersion) {
        this.formatVersion = formatVersion;
    }

    public int getNumberOfGeoLocations() {
        return numberOfGeoLocations;
    }

    public void setNumberOfGeoLocations(int numberOfGeoLocations) {
        this.numberOfGeoLocations = numberOfGeoLocations;
    }

    public int getNumberOfAccelerations() {
        return numberOfAccelerations;
    }

    public void setNumberOfAccelerations(int numberOfAccelerations) {
        this.numberOfAccelerations = numberOfAccelerations;
    }

    public int getNumberOfRotations() {
        return numberOfRotations;
    }

    public void setNumberOfRotations(int numberOfRotations) {
        this.numberOfRotations = numberOfRotations;
    }

    public int getNumberOfDirections() {
        return numberOfDirections;
    }

    public void setNumberOfDirections(int numberOfDirections) {
        this.numberOfDirections = numberOfDirections;
    }

    public int getBeginOfGeoLocationsIndex() {
        return beginOfGeoLocationsIndex;
    }

    public void setBeginOfGeoLocationsIndex(int beginOfGeoLocationsIndex) {
        this.beginOfGeoLocationsIndex = beginOfGeoLocationsIndex;
    }

    public int getBeginOfAccelerationsIndex() {
        return beginOfAccelerationsIndex;
    }

    public void setBeginOfAccelerationsIndex(int beginOfAccelerationsIndex) {
        this.beginOfAccelerationsIndex = beginOfAccelerationsIndex;
    }

    public int getBeginOfRotationsIndex() {
        return beginOfRotationsIndex;
    }

    public void setBeginOfRotationsIndex(int beginOfRotationsIndex) {
        this.beginOfRotationsIndex = beginOfRotationsIndex;
    }

    public int getBeginOfDirectionsIndex() {
        return beginOfDirectionsIndex;
    }

    public void setBeginOfDirectionsIndex(int beginOfDirectionsIndex) {
        this.beginOfDirectionsIndex = beginOfDirectionsIndex;
    }

    int beginOfDirectionsIndex;
}
