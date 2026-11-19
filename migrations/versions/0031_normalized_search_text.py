"""Store locale-independent projections of the published search text."""

import sqlalchemy as sa
from alembic import op

from app.services.procurement_search import indexed_search_text

revision = "0031_normalized_search_text"
down_revision = "0030_procurement_search_text"
branch_labels = None
depends_on = None

_FIELDS = {
    "procurements": ("object_description", "object_complement"),
    "procurement_items": ("description", "summary_description"),
}


def upgrade() -> None:
    bind = op.get_bind()
    for table_name, fields in _FIELDS.items():
        columns = {column["name"] for column in sa.inspect(bind).get_columns(table_name)}
        search_columns = [f"{name}_search" for name in fields]
        for name in search_columns:
            if name not in columns:
                op.add_column(table_name, sa.Column(name, sa.Text(), nullable=True))

        table = sa.table(
            table_name, *(sa.column(name) for name in ("id", *fields, *search_columns))
        )
        rows = sa.select(table.c.id, *(table.c[name] for name in fields)).where(
            sa.or_(*(table.c[name].is_not(None) for name in fields))
        )
        update = (
            table.update()
            .where(table.c.id == sa.bindparam("row_id"))
            .values({name: sa.bindparam(name) for name in search_columns})
        )
        last_id = None
        while True:
            batch = rows if last_id is None else rows.where(table.c.id > last_id)
            page = bind.execute(batch.order_by(table.c.id).limit(500)).mappings().all()
            if not page:
                break
            bind.execute(
                update,
                [
                    {
                        "row_id": row["id"],
                        **{f"{name}_search": indexed_search_text(row[name]) for name in fields},
                    }
                    for row in page
                ],
            )
            last_id = page[-1]["id"]


def downgrade() -> None:
    for table, fields in reversed(_FIELDS.items()):
        with op.batch_alter_table(table) as batch:
            for name in fields:
                batch.drop_column(f"{name}_search")
