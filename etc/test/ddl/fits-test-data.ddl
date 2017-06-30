insert into fits.unit(symbol, name) VALUES ('mm', 'millimetre');
insert into fits.type (typeID, name, description, unitPK) VALUES ('e', 'east', 'displacement from initial position', (select unitPK from fits.unit where symbol = 'mm'));
insert into fits.method (methodID, name, description, reference) VALUES ('bernese5', 'Bernese v5.0', 'Bernese v5.0 GNS processing software', 'http://info.geonet.org.nz/x/XoIW');
insert into fits.type_method (typePK, methodPK) VALUES ((select typePK from fits.type where typeID = 'e'), (select methodPK from fits.method where methodID = 'bernese5'));	
insert into fits.system(systemID, description) VALUES ('none', 'No external system reference');
insert into fits.sample(sampleID, systemPK) VALUES ('none', (select systemPK from fits.system where systemID = 'none'));
