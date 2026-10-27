"""No-overtaking reservations; actual multi-resource acquisition stays atomic."""


def copy_reservations(pending):
    owners = {}
    for order, resources in pending:
        for resource in resources:
            owners[resource] = min(order, owners.get(resource, order))
    return owners


def permitted(resources, order, reservations, *, acquired=False):
    return acquired or all(order <= reservations.get(resource, order) for resource in resources)
