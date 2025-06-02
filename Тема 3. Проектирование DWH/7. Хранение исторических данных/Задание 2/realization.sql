-- Удалите внешний ключ из sales
ALTER TABLE public.sales DROP CONSTRAINT sales_products_product_id_fk;

-- Удалите первичный ключ из products
ALTER TABLE public.products DROP CONSTRAINT products_pk;


-- Добавьте новое поле id для суррогантного ключа в products
ALTER TABLE public.products ADD COLUMN id SERIAL;

-- Сделайте данное поле первичным ключом
ALTER TABLE public.products ADD CONSTRAINT products_pk PRIMARY KEY (id);
-- Добавьте дату начала действия записи в products
ALTER TABLE public.products ADD COLUMN valid_from timestamptz NOT NULL;

-- Добавьте дату окончания действия записи в products
ALTER TABLE public.products ADD COLUMN valid_to timestamptz NOT NULL;


-- Добавьте новый внешний ключ sales_products_id_fk в sales
ALTER TABLE public.sales ADD CONSTRAINT sales_products_id_fk FOREIGN KEY(product_id) REFERENCES products(id);