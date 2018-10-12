# click-odoo-scaffold kick starts your click-odoo project


## To get started

	projectname=migrator
	git clone https://github.com/xoe-labs/click-odoo-scaffold \
	   click-odoo-${projectname} \
	&& cd click-odoo-${projectname} \
	&& make init


## To pull in updates

	make sync

_Note: Certain files are developped continously upstream._
 - `utils` falls under this category,
 - but also common scripts for `testing`.

If you improve one of the upstream files, consisder cherry-picking and propose
upstream for improvement.
