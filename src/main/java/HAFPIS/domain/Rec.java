package HAFPIS.domain;

import java.io.Serializable;

/**
 *
 * Created by ZP on 2017/5/15.
 */
public abstract class Rec<T extends Rec> implements Comparable<T>, Serializable {
    private static final long serialVersionUID = -8254622855028984547L;
    public String taskid;
    public String transno;
    public String probeid;
    public int dbid;
    public String candid;
    public int candrank;
    public float score;
    public int position;

    public int compareTo(T o) {
        return this.score > o.score ? -1 : (this.score < o.score ? 1 : 0);
    }
}
