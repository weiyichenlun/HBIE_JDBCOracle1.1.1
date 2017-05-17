package HAFPIS.domain;

import java.io.Serializable;

/**
 * Created by ZP on 2017/5/15.
 */
public class FaceRec extends Rec<FaceRec> implements Serializable {
    private static final long serialVersionUID = 7896766243363847702L;
    public float ffscores[];

    public FaceRec() {
        position = 0;
        ffscores = new float[3];
    }
}
