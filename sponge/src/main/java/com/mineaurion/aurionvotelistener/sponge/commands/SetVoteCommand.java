package com.mineaurion.aurionvotelistener.sponge.commands;

import com.mineaurion.aurionvotelistener.sponge.AurionVoteListener;
import org.spongepowered.api.command.CommandResult;
import org.spongepowered.api.command.CommandSource;
import org.spongepowered.api.command.args.CommandContext;
import org.spongepowered.api.command.spec.CommandExecutor;
import org.spongepowered.api.text.Text;

import java.sql.SQLException;

public class SetVoteCommand implements CommandExecutor {
    private AurionVoteListener plugin;

    public SetVoteCommand(AurionVoteListener plugin){
        this.plugin = plugin;
    }

    @Override
    public CommandResult execute(CommandSource src, CommandContext args){
        src.sendMessage(Text.of("This command has been deprecated"));
        return CommandResult.empty();
    }
}
