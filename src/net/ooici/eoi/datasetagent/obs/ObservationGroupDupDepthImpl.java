/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ooici.eoi.datasetagent.obs;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.ooici.eoi.netcdf.VariableParams;

/**
 * An IObservationGroup implementation which supports data observations with multiple observations at a given depth.  In effect,
 * this creates a duplicate set of observations at a given depth.
 * 
 * @author cmueller
 */
public class ObservationGroupDupDepthImpl extends AbstractObservationGroup {

	private HashMap<VariableParams, HashMap<TimeDepthPair, Number[]>> obsMap =
		new HashMap<VariableParams, HashMap<TimeDepthPair, Number[]>>();

	/**
     * Constructs a new ObservationGroupDupDepthImpl with the given identifying characteristics
     * 
     * @param id The identifier of this AbstractObservationGroup instance
     * @param stnid The identifier of the unit from which this AbstractObservationGroup's observations were made
     * @param lat The geographic latitude coordinate at which this AbstractObservationGroup's observations were made
     * @param lon The geographic longitude coordinate at which this AbstractObservationGroup's observations were made
     */
	public ObservationGroupDupDepthImpl(int id, String stnid, Number lat, Number lon) {
		super(id, stnid, lat, lon);
	}

	@Override
	protected void _addObservation(Number time, Number depth, Number data, VariableParams dataName) {
		if (!depths.contains(depth)) {
			depths.add(depth);
		}
		if (!times.contains(time)) {
			times.add(time);
		}
		if (!obsMap.containsKey(dataName)) {
			obsMap.put(dataName, new HashMap<TimeDepthPair, Number[]>());
		}
		TimeDepthPair cp = new TimeDepthPair(time, depth);

		/* Add value */
		Number[] num;
		Number[] cnum;
		if ((num = obsMap.get(dataName).get(cp)) != null) {
			cnum = new Number[num.length + 1];
			System.arraycopy(num, 0, cnum, 0, num.length);

			cnum[cnum.length - 1] = data;

		} else {
			cnum = new Number[] { data };
		}
		obsMap.get(dataName).put(cp, cnum);
	}
    
    @Override
    public boolean trimFirstTimestep() {
        boolean ret = false;
        if (times.size() > 0) {
            Number firstTime = times.get(0);
            /* For each of the VariableParams and for each depth, remove the corresponding time+depth entry */
            for (VariableParams vp : obsMap.keySet()) {
                for (Number d : depths) {
                    obsMap.get(vp).remove(new TimeDepthPair(firstTime, d));
                }
            }
            /* Remove the first time */
            times.remove(0);
            ret = true;
        }
        return ret;
    }

    @Override
	public int getNumObs() {
		return obsMap.get(obsMap.keySet().iterator().next()).size();
	}

    @Override
	public Number getData(VariableParams dataName, Number time, Number depth) {
		return getData(dataName, time, depth, Float.NaN);
	}

	public Number getData(VariableParams dataName, Number time, Number depth, boolean getAverage) {
		return getData(dataName, time, depth, Float.NaN, getAverage);
	}

    @Override
	public Number getData(VariableParams dataName, Number time, Number depth, Number missingVal) {
		return getData(dataName, time, depth, missingVal, true);
	}

	public Number getData(VariableParams dataName, Number time, Number depth, Number missingVal, boolean getAverage) {
	    double ret = Double.NaN;
		TimeDepthPair cp = new TimeDepthPair(time, depth);
		HashMap<TimeDepthPair, Number[]> map = obsMap.get(dataName);
		if (map != null) {
			Number[] fvals = map.get(cp);
			if (fvals != null && fvals.length > 0) {
				if (getAverage) {
					ret = fvals[0].floatValue();
					for (int in = 1; in < fvals.length; in++) {
						ret += fvals[in].doubleValue();
					}
					ret /= fvals.length;
				} else {
					ret = fvals[0].floatValue();
				}
			}
		}
		return (ret == Double.NaN) ? (missingVal) : (ret);
	}

    @Override
	public List<VariableParams> getDataNames() {
		return new ArrayList<VariableParams>(obsMap.keySet());
	}

	// @Override
	// public String toString() {
	// StringBuilder sb = new StringBuilder();
	// sb.append("allTimes{").append(allTimes.size()).append("} :: ").append(allTimes).append("\r\n");
	// sb.append("lats{").append(lats.size()).append("} :: ").append(lats).append("\r\n");
	// sb.append("lons{").append(lons.size()).append("} :: ").append(lons).append("\r\n");
	// sb.append("allDepths{").append(allDepths.size()).append("} :: ").append(allDepths).append("\r\n");
	// String[] keys = dataMap.keySet().toArray(new String[0]);
	// java.util.Arrays.sort(keys);
	//
	// int[] indices;
	// for (String i : keys) {
	// int[] inds = getIndices(i);
	// sb.append("{").append(inds[0]).append(",").append(inds[1]).append(",").append(inds[2]).append("}").append("[").append(dataMap.get(i)[0].size());
	// sb.append("] : ").append(getNcArray(i)).append("\r\n");
	// }
	//
	// return sb.toString();
	// }

}
