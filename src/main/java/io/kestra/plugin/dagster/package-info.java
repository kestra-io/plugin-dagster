@PluginSubGroup(
    title = "Dagster plugin",
    description = "This Plugin would bridge Kestra and Dagster by allowing flows to programmatically start Dagster runs, poll their statuses.",
    categories = {
        PluginSubGroup.PluginCategory.DATA
    }
)
package io.kestra.plugin.dagster;

import io.kestra.core.models.annotations.PluginSubGroup;