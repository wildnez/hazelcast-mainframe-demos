package com.hazelcast.certification.util;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.WanFilterEventType;
import com.hazelcast.map.wan.MapWanEventFilter;

public class WanFilterForMapStores implements MapWanEventFilter<String, String> {

    @Override
    public boolean filter(String s, EntryView<String, String> entryView, WanFilterEventType wanFilterEventType) {
        return false;
    }
}
