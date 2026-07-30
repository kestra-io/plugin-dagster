@PluginSubGroup(
    title = "Dagster",
    description = "This sub-group of plugins contains tasks for triggering Dagster jobs via the GraphQL API and polling their run status.",
    categories = {
        PluginSubGroup.PluginCategory.DATA
    }
)
package io.kestra.plugin.dagster;

import io.kestra.core.models.annotations.PluginSubGroup;