package com.mineaurion.aurionvotelistener.sponge.database;

import com.mineaurion.aurionvotelistener.sponge.config.Config;
import com.mineaurion.aurionvotelistener.sponge.AurionVoteListener;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DataSource {

    private AurionVoteListener plugin;
    private Config config;

    private DatabaseConnection connection;

    private String storageConfig;
    private String tableName;


    public DataSource(AurionVoteListener plugin) throws SQLException{
        this.plugin = plugin;
        this.config = plugin.getConfig();
        storageConfig = config.database.storage;
        tableName = config.database.prefix + config.database.tableName;

        if(storageConfig.equalsIgnoreCase("mysql")){
            connection = new MysqlDatabaseConnection(config);
        }
        else if(storageConfig.equalsIgnoreCase("sqlite")){
            connection = new SqliteDatabaseConnection(plugin.configDir + File.separator + config.database.file);
        }
        else{
            throw new IllegalArgumentException("Invalid Storage engine!");
        }
        prepareTable();
    }


    private synchronized void prepareTable(){
        try{
            String createTable = "CREATE TABLE `" + tableName + "` (`vote_id` varchar(100), `player_id` varchar(32) NOT NULL, `vote_service` varchar(64), `vote_timestamp` varchar(32), `player_ip` varchar(200), `vote_awarded` tinyint(1), PRIMARY KEY (`vote_id`));";
            if(!connection.tableExist(tableName)){
                connection.executeStatement(createTable);
                connection.getStatement().close();
                connection.getConnection().close();
                plugin.getLogger().info("Table created");
            }
        }
        catch (SQLException e){
            plugin.getLogger().error("Could not create Table!", e);
        }
    }

    public int totalsVote(String name){
        int votePlayer = 0;
        try(
            PreparedStatement sql = connection.getConnection().prepareStatement(
                String.format("SELECT COUNT(*) FROM %s WHERE `player_id`=?", tableName)
            );
            Connection connection = sql.getConnection()
        ){
            sql.setString(1, name);
            try(ResultSet resultSet = sql.executeQuery()){
                while (resultSet.next()) {
                    votePlayer = resultSet.getInt(1);
                }
            }
        }
        catch(SQLException e){
            plugin.getLogger().error("SQL error", e);
        }
        return votePlayer;
    }

    public String voteTop(){
        int place = 1;
        StringBuilder message = new StringBuilder();
        String messageFormat = config.settings.voteTop.format;
        try(
            PreparedStatement sql = connection.getConnection().prepareStatement(
                    String.format("SELECT *, COUNT(*) AS `total` FROM %s GROUP BY `player_id` ORDER BY `total` DESC limit ?", tableName)
            );
            Connection connection = sql.getConnection();
        ){
            sql.setLong(1, config.settings.voteTop.number);
            try(ResultSet resultSet = sql.executeQuery()) {
                while (resultSet.next()){
                    String user = resultSet.getString("player_id");
                    String total = String.valueOf(resultSet.getInt("total"));
                    message
                        .append(
                            messageFormat
                                .replace("<POSITION>", String.valueOf(place))
                                .replace("<TOTAL>", String.valueOf(total))
                                .replace("<username>", user)
                        )
                        .append("\n");
                    place++;
                }
            }
        }
        catch (SQLException e){
            plugin.getLogger().error("SQL error", e);
        }
        return message.toString();
    }

    public synchronized void clearTotals(){
        try(
            PreparedStatement sql = connection.getConnection().prepareStatement(String.format("DELETE FROM `%s` WHERE `vote_awarded`=1", tableName));
            Connection connection = sql.getConnection();
        ){
            sql.execute();
        }
        catch (SQLException e){
            plugin.getLogger().error("Clear Total Failed", e);
        }
    }

    public synchronized void clearQueue(){
        try(
            PreparedStatement sql = connection.getConnection().prepareStatement(String.format("DELETE FROM `%s` WHERE `vote_awarded`=0", tableName));
            Connection connection = sql.getConnection();
        ){
            sql.execute();
        }
        catch (SQLException e){
            plugin.getLogger().error("Clear Queue Failed", e);
        }
    }

    public void online(String player, String serviceName, String timeStamp, String address){
        try(
            PreparedStatement sql = connection.getPreparedStatement(String.format("INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(`vote_id`) DO UPDATE SET `vote_awarded`=1", tableName));
            Connection connection = sql.getConnection();
        ){
            String voteConcat = player + serviceName + timeStamp + address;
            sql.setString(1, UUID.nameUUIDFromBytes(voteConcat.getBytes()).toString());
            sql.setString(2, player);
            sql.setString(3, serviceName);
            sql.setString(4, timeStamp);
            sql.setString(5, address);
            sql.setBoolean(6, true);
            sql.executeUpdate();
        }
        catch(SQLException e){
            plugin.getLogger().error("SQL Error", e);
        }
    }

    public void offline(String player, String serviceName, String timeStamp, String address){
        try(
            PreparedStatement sql = connection.getPreparedStatement(String.format("INSERT IGNORE INTO %s VALUES (?, ?, ?, ?, ?, ?)", tableName));
            Connection connection = sql.getConnection();
        ){
            String voteConcat = player + serviceName + timeStamp + address;
            sql.setString(1, UUID.nameUUIDFromBytes(voteConcat.getBytes()).toString());
            sql.setString(2, player);
            sql.setString(3, serviceName);
            sql.setString(4, timeStamp);
            sql.setString(5, address);
            sql.setBoolean(6, false);
            sql.executeUpdate();
        }
        catch(SQLException e){
            plugin.getLogger().error("SQL Error", e);
        }
    }

    public boolean queueUsername(String player){
        try(
            PreparedStatement sql = connection.getPreparedStatement(String.format("SELECT * FROM %s WHERE `player_id`=? AND `vote_awarded`=0", tableName));
            Connection connection = sql.getConnection();
        ){
            sql.setString(1, player);
            try(ResultSet resultSet = sql.executeQuery()){
                if(!resultSet.next()){
                    return false;
                }
                else{
                    return true;
                }
            }
        }
        catch (SQLException e){
            plugin.getLogger().error("SQL Error", e);
            return false;
        }
    }

    public List<String> queueReward(String player){
        List<String> service = new ArrayList<String>();

        try(
            PreparedStatement sql = connection.getPreparedStatement(String.format("SELECT `vote_service` FROM %s WHERE `player_id`=? AND `vote_awarded`=0", tableName));
            Connection connection = sql.getConnection();
        ){
            sql.setString(1, player);
            ResultSet resultSet = sql.executeQuery();
            while (resultSet.next()){
                service.add(resultSet.getString("vote_service"));
            }
        }
        catch (SQLException e){
            plugin.getLogger().error("SQL Error", e);
        }
        return service;
    }

    public void removeQueue(String player, String service){
        try(
            PreparedStatement sql = connection.getPreparedStatement(String.format("UPDATE %s SET `vote_awarded`=1 WHERE `player_id`=? AND `vote_service`=?", tableName));
            Connection connection = sql.getConnection();
        ){
            sql.setString(1, player);
            sql.setString(2, service);
            sql.executeUpdate();
        }
        catch (SQLException e){
            plugin.getLogger().error("SQL Error", e);
        }
    }

    public List<String> queueAllPlayer(){
        List<String> player = new ArrayList<String>();
        try(
            PreparedStatement sql = connection.getPreparedStatement(String.format("SELECT `player_id` FROM `%s` WHERE `vote_awarded`=0", tableName));
            Connection connection = sql.getConnection();
        ){
            ResultSet resultSet = sql.executeQuery();
            while(resultSet.next()) {
                player.add(resultSet.getString("player_id"));
            }
        }
        catch (SQLException e){
            plugin.getLogger().error("SQL Error", e);
        }
        return player;
    }
}
