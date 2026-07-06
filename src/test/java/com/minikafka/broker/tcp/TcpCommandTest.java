package com.minikafka.broker.tcp;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TcpCommandTest {

    @Test
    void parsesSimpleCommandName() {
        TcpCommand command = TcpCommand.parse("topics");

        assertThat(command.name()).isEqualTo("TOPICS");
        assertThat(command.args()).isEmpty();
    }

    @Test
    void parsesCreateTopicArguments() {
        TcpCommand command = TcpCommand.parse("CREATE_TOPIC orders 3");

        assertThat(command.name()).isEqualTo("CREATE_TOPIC");
        assertThat(command.args()).containsExactly("orders", "3");
    }

    @Test
    void parsesProduceValueWithSpaces() {
        TcpCommand command = TcpCommand.parse("PRODUCE orders order-1 checkout completed");

        assertThat(command.args()).containsExactly("orders", "order-1", "checkout completed");
    }

    @Test
    void rejectsBlankCommand() {
        assertThatThrownBy(() -> TcpCommand.parse(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsProduceWithoutValue() {
        assertThatThrownBy(() -> TcpCommand.parse("PRODUCE orders key"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
