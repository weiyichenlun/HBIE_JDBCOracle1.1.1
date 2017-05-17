package HAFPIS.domain;

import java.io.Serializable;

/**
 * Created by ZP on 2017/5/15.
 */
public class IrisRec extends Rec<IrisRec> implements Serializable{
    private static final long serialVersionUID = -8839317836881261092L;
    public float iiscores[];

    public IrisRec() {
        position = 0;
        iiscores = new float[2];
    }
}
