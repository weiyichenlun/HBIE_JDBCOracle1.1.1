package HAFPIS.domain;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by ZP on 2017/5/15.
 */
public class SrchDataRec implements Externalizable{
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
    public byte[][] facemnt;
    public byte[][] irismnt;

    public int datatype;
    public int rpmntnum;
    public int fpmntnum;
    public int palmmntnum;
    public int facemntnum;
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
        facemnt = new byte[3][];
        irismnt = new byte[2][];

        rpmntnum = 0;
        fpmntnum = 0;
        palmmntnum = 0;
        facemntnum = 0;
        irismntnum = 0;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        System.out.println("now serializable begin...");
        out.write(probeId);
        for (int i = 0; i < RpMntLen.length; i++) {
            out.writeInt(RpMntLen[i]);
        }
        for (int i = 0; i < RpImgLen.length; i++) {
            out.writeInt(RpImgLen[i]);
        }
        for (int i = 0; i < FpMntLen.length; i++) {
            out.writeInt(FpMntLen[i]);
        }
        for (int i = 0; i < FpImgLen.length; i++) {
            out.writeInt(FpImgLen[i]);
        }
        for (int i = 0; i < PalmMntLen.length; i++) {
            out.writeInt(PalmMntLen[i]);
        }
        for (int i = 0; i < PalmImgLen.length; i++) {
            out.writeInt(PalmImgLen[i]);
        }
        for (int i = 0; i < FaceMntLen.length; i++) {
            out.writeInt(FaceMntLen[i]);
        }
        for (int i = 0; i < FaceImgLen.length; i++) {
            out.writeInt(FaceImgLen[i]);
        }
        for (int i = 0; i < IrisMntLen.length; i++) {
            out.writeInt(IrisMntLen[i]);
        }
        for (int i = 0; i < IrisImgLen.length; i++) {
            out.writeInt(IrisImgLen[i]);
        }
        for (int i = 0; i < reserved.length; i++) {
            out.writeInt(reserved[i]);
        }
        for (int i = 0; i < rpmnt.length; i++) {
            if (rpmnt[i] != null) {
                out.write(rpmnt[i]);
            }
        }
        for (int i = 0; i < fpmnt.length; i++) {
            if (fpmnt[i] != null) {
                out.write(fpmnt[i]);
            }
        }
        for (int i = 0; i < palmmnt.length; i++) {
            if (palmmnt[i] != null) {
                out.write(palmmnt[i]);
            }
        }
        for (int i = 0; i < irismnt.length; i++) {
            if (irismnt[i] != null) {
                out.write(irismnt[i]);
            }
        }

        for (int i = 0; i < facemnt.length; i++) {
            if (facemnt[i] != null) {
                out.write(facemnt[i]);
            }
        }
        if (latfpmnt != null && latfpmnt.length > 0) {
            out.write(latfpmnt);
        }
        if (latpalmmnt != null && latpalmmnt.length > 0) {
            out.write(latpalmmnt);
        }

//
//        if (facemnt != null && facemnt.length > 0) {
//            out.write(facemnt);
//        }
        out.flush();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        System.out.println("now deserializable begin...");
        in.readFully(probeId);
        for (int i = 0; i < RpMntLen.length; i++) {
            RpMntLen[i] = in.readInt();
        }
        for (int i = 0; i < RpImgLen.length; i++) {
            RpImgLen[i] = in.readInt();
        }
        for (int i = 0; i < PalmMntLen.length; i++) {
            PalmMntLen[i] = in.readInt();
        }
        for (int i = 0; i < PalmImgLen.length; i++) {
            PalmImgLen[i] = in.readInt();
        }
        for (int i=0; i<FaceMntLen.length; i++) {
            FaceMntLen[i] = in.readInt();
        }
        for (int i=0; i<FaceImgLen.length; i++) {
            FaceImgLen[i] = in.readInt();
        }
        for (int i = 0; i < IrisMntLen.length; i++) {
            IrisMntLen[i] = in.readInt();
        }
        for (int i = 0; i < IrisImgLen.length; i++) {
            IrisImgLen[i] = in.readInt();
        }
        for (int i = 0; i < reserved.length; i++) {
            reserved[i] = in.readInt();
        }

        for (int i = 0; i < rpmnt.length; i++) {
            in.readFully(rpmnt[i]);
        }
        for (int i = 0; i < fpmnt.length; i++) {
            in.readFully(fpmnt[i]);
        }
        for (int i = 0; i < palmmnt.length; i++) {
            in.readFully(palmmnt[i]);
        }
        for (int i = 0; i < irismnt.length; i++) {
            in.readFully(irismnt[i]);
        }
        for (int i = 0; i < facemnt.length; i++) {
            in.readFully(facemnt[i]);
        }
        in.readFully(latfpmnt);
        in.readFully(latpalmmnt);
//        in.readFully(facemnt);
    }
}
