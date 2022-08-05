'use strict';
module.exports = (sequelize, DataTypes) => {
  const mqtt_sensors = sequelize.define('mqtt_sensors', {
    termination_date: DataTypes.DATE,
    type: DataTypes.STRING,
    content: {
      type: DataTypes.JSONB,
      allowNull: true
    }
  }, {});

  return mqtt_sensors;
};