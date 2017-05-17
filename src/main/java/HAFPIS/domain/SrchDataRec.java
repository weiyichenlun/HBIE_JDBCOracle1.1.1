package HAFPIS.domain;

import java.io.Serializable;

/**
 * Created by ZP on 2017/5/15.
 */
public class SrchDataRec implements Serializable{
    private static final long serialVersionUID = -4662236686344805743L;
    public byte[] probeId;
    public int[] RpMntLen;
    public int[] RpImgLen;
    public int[] FpMntLen;
    public int[] FpImgLen;
    public int[] PalmMntLen;
    public int[] PalmImgLen;
    public int[] FaceMntLen;
    public int[] FaceImgLen;
    public int[] IrisMntLen;
    public int[] IrisImgLen;
    public int[] reserved;
    public byte[][] rpmnt;
    public byte[][] fpmnt;
    public byte[] latfpmnt;
    public byte[][] palmmnt;
    public byte[] latpalmmnt;
    public byte[] facemnt;
    public byte[][] irismnt;

    public int datatype;
    public int rpmntnum;
    public int fpmntnum;
    public int palmmntnum;
    public int irismntnum;

    public SrchDataRec() {
        probeId = new byte[33];
        RpMntLen = new int[10];
        RpImgLen = new int[10];
        FpMntLen = new int[10];
        FpImgLen = new int[10];
        PalmMntLen = new int[10];
        PalmImgLen = new int[10];
        FaceMntLen = new int[3];
        FaceImgLen = new int[3];
        IrisMntLen = new int[2];
        IrisImgLen = new int[2];
        reserved = new int[10];
        rpmnt = new byte[10][];
        fpmnt = new byte[10][];
        latfpmnt = new byte[0];
        palmmnt = new byte[4][];
        latpalmmnt = new byte[0];
        facemnt = new byte[0];
        irismnt = new byte[2][];

        rpmntnum = 0;
        fpmntnum = 0;
        palmmntnum = 0;
        irismntnum = 0;
    }
}
