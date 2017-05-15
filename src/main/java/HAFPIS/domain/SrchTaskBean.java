package HAFPIS.domain;

import java.sql.Blob;
import java.sql.Clob;

/**
 * Created by ZP on 2017/5/15.
 */
public class SrchTaskBean {
    private String TASKIDD;
    private String TRANSNO;
    private String PROBEID;
    private int DATATYPE;
    private int TASKTYPE;
    private int STATUS;
    private int PRIORITY;
    private int USERID;
    private String IPAddr;
    private int NUMOFCAND;
    private int ROTATION;
    private int COREMODE;
    private double NCORERANGE;
    private double UCORERANGE;
    private double LDELTARANGE;
    private double RDELTARANGE;
    private double RCSRCHFLAG;
    private double TEXTUREFLAG;
    private int AVERAGECAND;
    private int PATTERNFILTER;
    private String SRCHPOSMASK;
    private String SRCHDBSMASK;
    private int PMRECTX01;
    private int PMRECTY01;
    private int PMRECTW01;
    private int PMRECTH01;
    private int PMRECTX06;
    private int PMRECTY06;
    private int PMRECTW06;
    private int PMRECTH06;
    private int PMRECTX05;
    private int PMRECTY05;
    private int PMRECTW05;
    private int PMRECTH05;
    private int PMRECTX010;
    private int PMRECTY010;
    private int PMRECTW010;
    private int PMRECTH010;
    private Clob DEMOFILTER;
    private Blob SRCHDATA;
    private String BEGTIME;
    private String ENDTIME;
    private String EXPTMSG;

    public String getTASKIDD() {
        return TASKIDD;
    }

    public void setTASKIDD(String TASKIDD) {
        this.TASKIDD = TASKIDD;
    }

    public String getTRANSNO() {
        return TRANSNO;
    }

    public void setTRANSNO(String TRANSNO) {
        this.TRANSNO = TRANSNO;
    }

    public String getPROBEID() {
        return PROBEID;
    }

    public void setPROBEID(String PROBEID) {
        this.PROBEID = PROBEID;
    }

    public int getDATATYPE() {
        return DATATYPE;
    }

    public void setDATATYPE(int DATATYPE) {
        this.DATATYPE = DATATYPE;
    }

    public int getTASKTYPE() {
        return TASKTYPE;
    }

    public void setTASKTYPE(int TASKTYPE) {
        this.TASKTYPE = TASKTYPE;
    }

    public int getSTATUS() {
        return STATUS;
    }

    public void setSTATUS(int STATUS) {
        this.STATUS = STATUS;
    }

    public int getPRIORITY() {
        return PRIORITY;
    }

    public void setPRIORITY(int PRIORITY) {
        this.PRIORITY = PRIORITY;
    }

    public int getUSERID() {
        return USERID;
    }

    public void setUSERID(int USERID) {
        this.USERID = USERID;
    }

    public String getIPAddr() {
        return IPAddr;
    }

    public void setIPAddr(String IPAddr) {
        this.IPAddr = IPAddr;
    }

    public int getNUMOFCAND() {
        return NUMOFCAND;
    }

    public void setNUMOFCAND(int NUMOFCAND) {
        this.NUMOFCAND = NUMOFCAND;
    }

    public int getROTATION() {
        return ROTATION;
    }

    public void setROTATION(int ROTATION) {
        this.ROTATION = ROTATION;
    }

    public int getCOREMODE() {
        return COREMODE;
    }

    public void setCOREMODE(int COREMODE) {
        this.COREMODE = COREMODE;
    }

    public double getNCORERANGE() {
        return NCORERANGE;
    }

    public void setNCORERANGE(double NCORERANGE) {
        this.NCORERANGE = NCORERANGE;
    }

    public double getUCORERANGE() {
        return UCORERANGE;
    }

    public void setUCORERANGE(double UCORERANGE) {
        this.UCORERANGE = UCORERANGE;
    }

    public double getLDELTARANGE() {
        return LDELTARANGE;
    }

    public void setLDELTARANGE(double LDELTARANGE) {
        this.LDELTARANGE = LDELTARANGE;
    }

    public double getRDELTARANGE() {
        return RDELTARANGE;
    }

    public void setRDELTARANGE(double RDELTARANGE) {
        this.RDELTARANGE = RDELTARANGE;
    }

    public double getRCSRCHFLAG() {
        return RCSRCHFLAG;
    }

    public void setRCSRCHFLAG(double RCSRCHFLAG) {
        this.RCSRCHFLAG = RCSRCHFLAG;
    }

    public double getTEXTUREFLAG() {
        return TEXTUREFLAG;
    }

    public void setTEXTUREFLAG(double TEXTUREFLAG) {
        this.TEXTUREFLAG = TEXTUREFLAG;
    }

    public int getAVERAGECAND() {
        return AVERAGECAND;
    }

    public void setAVERAGECAND(int AVERAGECAND) {
        this.AVERAGECAND = AVERAGECAND;
    }

    public int getPATTERNFILTER() {
        return PATTERNFILTER;
    }

    public void setPATTERNFILTER(int PATTERNFILTER) {
        this.PATTERNFILTER = PATTERNFILTER;
    }

    public String getSRCHPOSMASK() {
        return SRCHPOSMASK;
    }

    public void setSRCHPOSMASK(String SRCHPOSMASK) {
        this.SRCHPOSMASK = SRCHPOSMASK;
    }

    public String getSRCHDBSMASK() {
        return SRCHDBSMASK;
    }

    public void setSRCHDBSMASK(String SRCHDBSMASK) {
        this.SRCHDBSMASK = SRCHDBSMASK;
    }

    public int getPMRECTX01() {
        return PMRECTX01;
    }

    public void setPMRECTX01(int PMRECTX01) {
        this.PMRECTX01 = PMRECTX01;
    }

    public int getPMRECTY01() {
        return PMRECTY01;
    }

    public void setPMRECTY01(int PMRECTY01) {
        this.PMRECTY01 = PMRECTY01;
    }

    public int getPMRECTW01() {
        return PMRECTW01;
    }

    public void setPMRECTW01(int PMRECTW01) {
        this.PMRECTW01 = PMRECTW01;
    }

    public int getPMRECTH01() {
        return PMRECTH01;
    }

    public void setPMRECTH01(int PMRECTH01) {
        this.PMRECTH01 = PMRECTH01;
    }

    public int getPMRECTX06() {
        return PMRECTX06;
    }

    public void setPMRECTX06(int PMRECTX06) {
        this.PMRECTX06 = PMRECTX06;
    }

    public int getPMRECTY06() {
        return PMRECTY06;
    }

    public void setPMRECTY06(int PMRECTY06) {
        this.PMRECTY06 = PMRECTY06;
    }

    public int getPMRECTW06() {
        return PMRECTW06;
    }

    public void setPMRECTW06(int PMRECTW06) {
        this.PMRECTW06 = PMRECTW06;
    }

    public int getPMRECTH06() {
        return PMRECTH06;
    }

    public void setPMRECTH06(int PMRECTH06) {
        this.PMRECTH06 = PMRECTH06;
    }

    public int getPMRECTX05() {
        return PMRECTX05;
    }

    public void setPMRECTX05(int PMRECTX05) {
        this.PMRECTX05 = PMRECTX05;
    }

    public int getPMRECTY05() {
        return PMRECTY05;
    }

    public void setPMRECTY05(int PMRECTY05) {
        this.PMRECTY05 = PMRECTY05;
    }

    public int getPMRECTW05() {
        return PMRECTW05;
    }

    public void setPMRECTW05(int PMRECTW05) {
        this.PMRECTW05 = PMRECTW05;
    }

    public int getPMRECTH05() {
        return PMRECTH05;
    }

    public void setPMRECTH05(int PMRECTH05) {
        this.PMRECTH05 = PMRECTH05;
    }

    public int getPMRECTX010() {
        return PMRECTX010;
    }

    public void setPMRECTX010(int PMRECTX010) {
        this.PMRECTX010 = PMRECTX010;
    }

    public int getPMRECTY010() {
        return PMRECTY010;
    }

    public void setPMRECTY010(int PMRECTY010) {
        this.PMRECTY010 = PMRECTY010;
    }

    public int getPMRECTW010() {
        return PMRECTW010;
    }

    public void setPMRECTW010(int PMRECTW010) {
        this.PMRECTW010 = PMRECTW010;
    }

    public int getPMRECTH010() {
        return PMRECTH010;
    }

    public void setPMRECTH010(int PMRECTH010) {
        this.PMRECTH010 = PMRECTH010;
    }

    public Clob getDEMOFILTER() {
        return DEMOFILTER;
    }

    public void setDEMOFILTER(Clob DEMOFILTER) {
        this.DEMOFILTER = DEMOFILTER;
    }

    public Blob getSRCHDATA() {
        return SRCHDATA;
    }

    public void setSRCHDATA(Blob SRCHDATA) {
        this.SRCHDATA = SRCHDATA;
    }

    public String getBEGTIME() {
        return BEGTIME;
    }

    public void setBEGTIME(String BEGTIME) {
        this.BEGTIME = BEGTIME;
    }

    public String getENDTIME() {
        return ENDTIME;
    }

    public void setENDTIME(String ENDTIME) {
        this.ENDTIME = ENDTIME;
    }

    public String getEXPTMSG() {
        return EXPTMSG;
    }

    public void setEXPTMSG(String EXPTMSG) {
        this.EXPTMSG = EXPTMSG;
    }
}
